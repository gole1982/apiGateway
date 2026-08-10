package logger

type LogConfig struct {
	QueueCapacity   int
	MaxAgeDays      int
	MaxRecords      int
	CleanupInterval int
	MaxBodySizeKB   int
	BatchSize       int
	BatchIntervalMS int
}

func DefaultLogConfig() LogConfig {
	return LogConfig{
		QueueCapacity:   1000,
		MaxAgeDays:      30,
		MaxRecords:      100000,
		CleanupInterval: 24,
		MaxBodySizeKB:   50,
		BatchSize:       100,
		BatchIntervalMS: 1000,
	}
}
