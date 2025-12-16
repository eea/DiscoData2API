using System.Collections.Concurrent;

namespace DiscoData2API_Priv.Services
{
    public class CircuitBreakerService
    {
        private readonly ILogger<CircuitBreakerService> _logger;
        private readonly ConcurrentDictionary<string, CircuitBreaker> _circuitBreakers;

        public CircuitBreakerService(ILogger<CircuitBreakerService> logger)
        {
            _logger = logger;
            _circuitBreakers = new ConcurrentDictionary<string, CircuitBreaker>();
        }

        public CircuitBreaker GetCircuitBreaker(string name, int failureThreshold = 5, TimeSpan? timeout = null)
        {
            return _circuitBreakers.GetOrAdd(name, _ => new CircuitBreaker(name, failureThreshold, timeout ?? TimeSpan.FromMinutes(1), _logger));
        }
    }

    public class CircuitBreaker
    {
        private readonly string _name;
        private readonly int _failureThreshold;
        private readonly TimeSpan _timeout;
        private readonly ILogger _logger;
        private readonly object _lock = new object();

        private int _failureCount = 0;
        private DateTime _lastFailureTime = DateTime.MinValue;
        private CircuitBreakerState _state = CircuitBreakerState.Closed;

        public CircuitBreaker(string name, int failureThreshold, TimeSpan timeout, ILogger logger)
        {
            _name = name;
            _failureThreshold = failureThreshold;
            _timeout = timeout;
            _logger = logger;
        }

        public async Task<T> ExecuteAsync<T>(Func<Task<T>> operation)
        {
            if (GetState() == CircuitBreakerState.Open)
            {
                throw new CircuitBreakerOpenException($"Circuit breaker '{_name}' is open");
            }

            try
            {
                var result = await operation();
                OnSuccess();
                return result;
            }
            catch (Exception ex)
            {
                OnFailure(ex);
                throw;
            }
        }

        public T Execute<T>(Func<T> operation)
        {
            if (GetState() == CircuitBreakerState.Open)
            {
                throw new CircuitBreakerOpenException($"Circuit breaker '{_name}' is open");
            }

            try
            {
                var result = operation();
                OnSuccess();
                return result;
            }
            catch (Exception ex)
            {
                OnFailure(ex);
                throw;
            }
        }

        private CircuitBreakerState GetState()
        {
            lock (_lock)
            {
                if (_state == CircuitBreakerState.Open)
                {
                    if (DateTime.UtcNow - _lastFailureTime >= _timeout)
                    {
                        _state = CircuitBreakerState.HalfOpen;
                        _logger.LogInformation($"Circuit breaker '{_name}' transitioning to half-open state");
                    }
                }

                return _state;
            }
        }

        private void OnSuccess()
        {
            lock (_lock)
            {
                if (_state == CircuitBreakerState.HalfOpen)
                {
                    _state = CircuitBreakerState.Closed;
                    _failureCount = 0;
                    _logger.LogInformation($"Circuit breaker '{_name}' closed after successful operation");
                }
                else if (_state == CircuitBreakerState.Closed && _failureCount > 0)
                {
                    _failureCount = 0;
                    _logger.LogDebug($"Circuit breaker '{_name}' failure count reset");
                }
            }
        }

        private void OnFailure(Exception ex)
        {
            lock (_lock)
            {
                _failureCount++;
                _lastFailureTime = DateTime.UtcNow;

                if (_failureCount >= _failureThreshold)
                {
                    _state = CircuitBreakerState.Open;
                    _logger.LogWarning($"Circuit breaker '{_name}' opened after {_failureCount} failures. Last error: {ex.Message}");
                }
                else
                {
                    _logger.LogDebug($"Circuit breaker '{_name}' failure count: {_failureCount}/{_failureThreshold}");
                }
            }
        }

        public CircuitBreakerState State => GetState();
        public int FailureCount => _failureCount;
        public string Name => _name;
    }

    public enum CircuitBreakerState
    {
        Closed,   // Normal operation
        Open,     // Blocking calls
        HalfOpen  // Testing if service is back
    }

    public class CircuitBreakerOpenException : Exception
    {
        public CircuitBreakerOpenException(string message) : base(message) { }
        public CircuitBreakerOpenException(string message, Exception innerException) : base(message, innerException) { }
    }
}