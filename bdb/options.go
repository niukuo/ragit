package bdb

type StorageOptions func(*storage)

func WithWaitApplyCap(cap int) StorageOptions {
	return func(s *storage) {
		s.waitApplyCap = cap
	}
}

func WithSubscriberCap(cap int) StorageOptions {
	return func(s *storage) {
		s.subCap = cap
	}
}
