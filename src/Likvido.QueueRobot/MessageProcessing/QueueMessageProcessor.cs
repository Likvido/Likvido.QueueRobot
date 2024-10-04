using System.Reflection;
using System.Text.Json;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Likvido.CloudEvents;
using Likvido.QueueRobot.Exceptions;
using Likvido.QueueRobot.JsonConverters;
using Likvido.QueueRobot.MessageHandling;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;

namespace Likvido.QueueRobot.MessageProcessing;

internal sealed class QueueMessageProcessor : IDisposable
{
    private readonly ILogger _logger;
    private readonly QueueRobotOptions _workerOptions;
    private readonly IServiceProvider _serviceProvider;
    private readonly ResiliencePipeline _deleteMessageResiliencePipeline;
    private readonly ResiliencePipeline _updateMessageResiliencyPipeline;
    private readonly SemaphoreSlim _messageReceiptSemaphore = new(1, 1);
    private readonly TelemetryClient _telemetryClient;
    private readonly string _queueName;
    private readonly QueueClient _queueClient;

    private QueueClient? _poisonQueueClient;

    private string PoisonQueueName => $"{_queueName}-poison";
    private QueueClient PoisonQueueClient =>
        _poisonQueueClient ??= new QueueClient(_workerOptions.AzureStorageConnectionString, PoisonQueueName);

    public QueueMessageProcessor(
        ILogger logger,
        QueueClient queueClient,
        IServiceProvider serviceProvider,
        QueueRobotOptions workerOptions,
        TelemetryClient telemetryClient,
        string queueName)
    {
        _logger = logger;
        _queueClient = queueClient;
        _workerOptions = workerOptions;
        _serviceProvider = serviceProvider;
        _deleteMessageResiliencePipeline = GetMessageActionResiliencePipeline("Message deletion from a queue \"{queueName}\" failed #{retryAttempt}");
        _updateMessageResiliencyPipeline = GetMessageActionResiliencePipeline("Message update in a queue \"{queueName}\" failed #{retryAttempt}");
        _telemetryClient = telemetryClient;
        _queueName = queueName;
    }

    public async Task ProcessMessage(QueueMessage queueMessage, LikvidoPriority priority, CancellationToken stoppingToken)
    {
        ArgumentNullException.ThrowIfNull(queueMessage);

        //Running this as separate task gives the following benefits
        //1. ExecutionContext isn't overlap between message handlers
        //2. Makes message parallel processing easier
        await Task.Yield();//makes execution parallel immediately for the reasons above

        var processed = false;
        MessageDetails? messageDetails = null;
        IServiceScope? scope = null;
        IAsyncDisposable? updateVisibilityStopAction = null;
        IOperationHolder<RequestTelemetry>? operation = null;
        try
        {
            messageDetails = new MessageDetails(queueMessage);

            operation = _telemetryClient.StartOperation<RequestTelemetry>($"Process {_queueName}");
            operation.Telemetry.Properties["InvocationId"] = queueMessage.MessageId;
            operation.Telemetry.Properties["MessageId"] = queueMessage.MessageId;
            operation.Telemetry.Properties["OperationName"] = $"Process {_queueName}";
            operation.Telemetry.Properties["TriggerReason"] = $"New queue message detected on '{_queueName}'.";
            operation.Telemetry.Properties["QueueName"] = _queueName;
            operation.Telemetry.Properties["Priority"] = Enum.GetName(priority);
            operation.Telemetry.Properties["Robot"] = Assembly.GetEntryAssembly()?.GetName().Name;

            updateVisibilityStopAction = await StartKeepMessageInvisibleAsync(_queueClient, messageDetails);

            scope = _serviceProvider.CreateScope();
            await ProcessMessage(messageDetails, priority, scope, stoppingToken);

            stoppingToken.ThrowIfCancellationRequested();

            processed = true;
            _telemetryClient.TrackTrace("Processor finished"); //short cut for now. TOOD:// wrap processing in a separate operation

            await DeleteMessageAsync(_queueClient, messageDetails, updateVisibilityStopAction);
            operation.Telemetry.Success = true;
            operation.Telemetry.ResponseCode = "0";
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Operation was cancelled.");
            if (operation != null)
            {
                operation.Telemetry.Success = false;
                operation.Telemetry.ResponseCode = "400";
            }
        }
        catch (PostponeProcessingException postponeProcessingException)
        {
            _logger.LogInformation(postponeProcessingException, "Postpone processing for {PostponeTime}", postponeProcessingException.PostponeTime);

            if (!processed && messageDetails != null)
            {
                await UpdateVisibilityTimeout(_queueClient, messageDetails, postponeProcessingException.PostponeTime, stoppingToken);
            }

            if (operation != null)
            {
                operation.Telemetry.Success = false;
                operation.Telemetry.ResponseCode = "202";
            }
        }
        catch (Exception ex)
        {
            if (!processed && messageDetails != null)
            {
                await TryMoveToPoisonAsync(_queueClient, messageDetails, updateVisibilityStopAction);
            }

            _logger.LogError(ex, "Unhandled exception occurred during message processing.");
            if (operation != null)
            {
                operation.Telemetry.Success = false;
                operation.Telemetry.ResponseCode = "500";
            }
        }
        finally
        {
            if (updateVisibilityStopAction != null)
            {
                await updateVisibilityStopAction.DisposeAsync();
            }
            scope?.Dispose();
            operation?.Dispose();
        }
    }

    private async Task ProcessMessage(MessageDetails messageDetails, LikvidoPriority priority, IServiceScope scope, CancellationToken stoppingToken)
    {
        var (messageType, handlerType) = GetDataTypes(messageDetails.Message);
        var cloudEventType = typeof(CloudEvent<>).MakeGenericType(messageType);
        var lastAttempt = messageDetails.Message.DequeueCount >= _workerOptions.MaxRetryCount;
        var jsonSerializerOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            AllowTrailingCommas = true,
            ReadCommentHandling = JsonCommentHandling.Skip
        };
        jsonSerializerOptions.Converters.Add(new LikvidoPriorityConverter());

        var message = JsonSerializer.Deserialize(
            messageDetails.Message.GetMessageText(),
            cloudEventType,
            jsonSerializerOptions)!;

        var messageHandler = (IMessageHandlerBase)scope.ServiceProvider.GetRequiredService(handlerType);
        await messageHandler.HandleMessage(message, priority, lastAttempt, stoppingToken);
    }

    private (Type MessageType, Type HandlerType) GetDataTypes(QueueMessage message)
    {
        var messageText = message.GetMessageText();
        var document = JsonDocument.Parse(messageText);

        var type =
            document.RootElement.EnumerateObject()
                .Where(property => string.Equals(property.Name, "Type", StringComparison.OrdinalIgnoreCase))
                .Select(property => property.Value.GetString()).FirstOrDefault();

        if (type == null || !_workerOptions.EventTypeHandlerDictionary.TryGetValue(type, out var dataType))
        {
            if (!_workerOptions.EventTypeHandlerDictionary.TryGetValue("*", out dataType))
            {
                throw new InvalidOperationException($"Event type is not registered in the {nameof(_workerOptions.EventTypeHandlerDictionary)}: '{type}'");
            }
        }

        return dataType;
    }

    private async Task DeleteMessageAsync(
        QueueClient queueClient,
        MessageDetails messageDetails,
        IAsyncDisposable? updateVisibilityStopAction)
    {
        await ModifyMessageAsync(async () =>
        {
            if (updateVisibilityStopAction != null)
            {
                await updateVisibilityStopAction.DisposeAsync();
            }
            await _deleteMessageResiliencePipeline
                .ExecuteAsync(async cancellationToken =>
                    await queueClient.DeleteMessageAsync(messageDetails.Message.MessageId, messageDetails.Receipt, cancellationToken));
        });
    }

    private async Task TryMoveToPoisonAsync(
        QueueClient queueClient,
        MessageDetails messageDetails,
        IAsyncDisposable? updateVisibilityStopAction)
    {
        if (messageDetails.Message.DequeueCount >= _workerOptions.MaxRetryCount)
        {
            try
            {
                await PoisonQueueClient.AddMessageAndCreateIfNotExistsAsync(messageDetails.Message.MessageText);
                await DeleteMessageAsync(queueClient, messageDetails, updateVisibilityStopAction);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "TryMoveToPoisonAsync failed. Poison queue: {PoisonQueue}", PoisonQueueName);
            }
        }
    }

    private Task<IAsyncDisposable> StartKeepMessageInvisibleAsync(
        QueueClient queueClient,
        MessageDetails messageDetails)
    {
#pragma warning disable CA2000 // Dispose objects before losing scope
        var updateMessageTokenSource = new CancellationTokenSource();
#pragma warning restore CA2000 // Dispose objects before losing scope
        var updateQueueMessageTask = KeepMessageInvisibleAsync(queueClient, messageDetails, updateMessageTokenSource.Token);

        var disposeAction = new DisposeAction(async () =>
        {
            await updateMessageTokenSource.CancelAsync();
            updateMessageTokenSource.Dispose();
            await updateQueueMessageTask;
        });

        return Task.FromResult((IAsyncDisposable)disposeAction);
    }

    private async Task KeepMessageInvisibleAsync(
        QueueClient queueClient,
        MessageDetails messageDetails,
        CancellationToken token)
    {
        var sleep = _workerOptions.VisibilityTimeout.TotalSeconds > 15
            ? _workerOptions.VisibilityTimeout.Subtract(TimeSpan.FromSeconds(10))
            : _workerOptions.VisibilityTimeout.Divide(2);
        try
        {
            do
            {
                await Task.Delay(sleep, token);
                await UpdateVisibilityTimeout(queueClient, messageDetails, _workerOptions.VisibilityTimeout, token);
            } while (true);
        }
        catch (OperationCanceledException)
        {
            //skip
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Update message visibility has failed.");
        }
    }

    private async Task UpdateVisibilityTimeout(QueueClient queueClient, MessageDetails messageDetails, TimeSpan newVisibilityTimeout, CancellationToken token) =>
        await ModifyMessageAsync(async () =>
        {
            var result = await _updateMessageResiliencyPipeline.ExecuteAsync(async cancellationToken =>
                await queueClient.UpdateMessageAsync(
                    messageDetails.Message.MessageId,
                    messageDetails.Receipt,
                    messageDetails.Message.MessageText,
                    newVisibilityTimeout,
                    cancellationToken), token);

            //all further operations should be done with the new receipt otherwise we get 404
            messageDetails.Receipt = result.Value.PopReceipt;
        }, token);

    private ResiliencePipeline GetMessageActionResiliencePipeline(string failureText) =>
        new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder().Handle<Exception>(),
                MaxRetryAttempts = 3,
                Delay = TimeSpan.FromSeconds(2),
                BackoffType = DelayBackoffType.Exponential,
                OnRetry = args =>
                {
                    _logger.LogError(args.Outcome.Exception, failureText, _queueName, args.AttemptNumber);
                    return default;
                }
            })
            .Build();

    /// <summary>
    /// All operations to a message need to be done via this helper function
    /// Each message update changes the message receipt and causes a 404 result with a previous receipt 
    /// </summary>
    /// <param name="callback"></param>
    /// <param name="token"></param>
    /// <returns></returns>
    private async Task ModifyMessageAsync(Func<Task> callback, CancellationToken token = default)
    {
        bool semaphoreCaptured = false;
        try
        {
            await _messageReceiptSemaphore.WaitAsync(token);
            semaphoreCaptured = true;
            await callback();
        }
        finally
        {
            if (semaphoreCaptured)
            {
                _messageReceiptSemaphore.Release();
            }
        }
    }

    public void Dispose()
    {
        _messageReceiptSemaphore.Dispose();
    }

    /// <summary>
    /// We need this because the message receipt changes after each operation
    /// </summary>
    private class MessageDetails(QueueMessage message)
    {
        public QueueMessage Message { get; } = message;
        public string Receipt { get; set; } = message.PopReceipt;
    }
}
