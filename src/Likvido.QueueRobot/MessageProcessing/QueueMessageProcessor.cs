using System.Reflection;
using System.Text.Json;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Likvido.CloudEvents;
using Likvido.QueueRobot.Exceptions;
using Likvido.QueueRobot.JsonConverters;
using Likvido.QueueRobot.MessageHandling;
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
        string queueName)
    {
        _logger = logger;
        _queueClient = queueClient;
        _workerOptions = workerOptions;
        _serviceProvider = serviceProvider;
        _deleteMessageResiliencePipeline = GetMessageActionResiliencePipeline("Message deletion from a queue \"{queueName}\" failed #{retryAttempt}");
        _updateMessageResiliencyPipeline = GetMessageActionResiliencePipeline("Message update in a queue \"{queueName}\" failed #{retryAttempt}");
        _queueName = queueName;
    }

    public async Task ProcessMessage(QueueMessage queueMessage, LikvidoPriority priority, CancellationToken stoppingToken)
    {
        ArgumentNullException.ThrowIfNull(queueMessage);

        // Forcing this to run as a separate task ensures that the ExecutionContext does not overlap between message handlers
        await Task.Yield();

        using var _ = _logger.BeginScope(new Dictionary<string, object>
        {
            ["MessageId"] = queueMessage.MessageId,
            ["QueueName"] = _queueName,
            ["Priority"] = Enum.GetName(priority) ?? "Unknown"
        });

        var processed = false;
        MessageDetails? messageDetails = null;
        IServiceScope? scope = null;
        IAsyncDisposable? updateVisibilityStopAction = null;
        try
        {
            messageDetails = new MessageDetails(queueMessage);

            updateVisibilityStopAction = await StartKeepMessageInvisibleAsync(_queueClient, messageDetails);

            scope = _serviceProvider.CreateScope();
            await ProcessMessage(messageDetails, priority, scope, stoppingToken);

            stoppingToken.ThrowIfCancellationRequested();

            processed = true;

            await DeleteMessageAsync(_queueClient, messageDetails, updateVisibilityStopAction);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Operation was cancelled.");
        }
        catch (PostponeProcessingException postponeProcessingException)
        {
            _logger.LogInformation(postponeProcessingException, "Postpone processing for {PostponeTime}", postponeProcessingException.PostponeTime);

            if (!processed && messageDetails != null)
            {
                await UpdateVisibilityTimeout(_queueClient, messageDetails, postponeProcessingException.PostponeTime, stoppingToken);
            }
        }
        catch (Exception ex)
        {
            using var exceptionScope = _logger.BeginScope(new Dictionary<string, object>
            {
                ["MessageText"] = messageDetails?.Message.GetMessageText(_logger) ?? "Message text is not available"
            });

            if (messageDetails == null)
            {
                _logger.LogError(ex, "Unhandled exception occurred during message processing, and messageDetails is null.");
            }
            else if (IsLastAttempt(messageDetails))
            {
                if (processed)
                {
                    _logger.LogError(ex, "Unhandled exception occurred during message processing. Message was successfully processed.");
                }
                else
                {
                    _logger.LogError(ex, "Unhandled exception occurred during message processing. Message was not processed, and it will be moved to the poison queue.");
                    await TryMoveToPoisonAsync(_queueClient, messageDetails, updateVisibilityStopAction);
                }
            }
            else
            {
                if (processed)
                {
                    _logger.LogError(ex, "Unhandled exception occurred during message processing. Message was successfully processed.");
                }
                else
                {
                    _logger.LogWarning(ex, "Unhandled exception occurred during message processing. Message will be retried within {VisibilityTimeoutTotalSeconds} seconds.", _workerOptions.VisibilityTimeout.TotalSeconds);
                }
            }
        }
        finally
        {
            if (updateVisibilityStopAction != null)
            {
                await updateVisibilityStopAction.DisposeAsync();
            }

            scope?.Dispose();
        }
    }

    private async Task ProcessMessage(MessageDetails messageDetails, LikvidoPriority priority, IServiceScope scope, CancellationToken stoppingToken)
    {
        var (messageType, handlerType) = GetDataTypes(messageDetails.Message);
        var cloudEventType = typeof(CloudEvent<>).MakeGenericType(messageType);
        var jsonSerializerOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            AllowTrailingCommas = true,
            ReadCommentHandling = JsonCommentHandling.Skip
        };
        jsonSerializerOptions.Converters.Add(new LikvidoPriorityConverter());

        var message = JsonSerializer.Deserialize(
            messageDetails.Message.GetMessageText(_logger),
            cloudEventType,
            jsonSerializerOptions)!;

        var messageHandler = (IMessageHandlerBase)scope.ServiceProvider.GetRequiredService(handlerType);
        await messageHandler.HandleMessage(message, priority, IsLastAttempt(messageDetails), stoppingToken);
    }

    private bool IsLastAttempt(MessageDetails messageDetails)
    {
        return messageDetails.Message.DequeueCount >= _workerOptions.MaxRetryCount;
    }

    private (Type MessageType, Type HandlerType) GetDataTypes(QueueMessage message)
    {
        var messageText = message.GetMessageText(_logger);
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
                    // Don't log OperationCanceledExceptions since these are handled
                    if(args.Outcome.Exception is not OperationCanceledException)
                    {
                        _logger.LogError(args.Outcome.Exception, failureText, _queueName, args.AttemptNumber);
                    }
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
