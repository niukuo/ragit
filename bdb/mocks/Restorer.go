// Code generated by mockery v2.23.0. DO NOT EDIT.

package mocks

import (
	bdb "github.com/niukuo/ragit/bdb"
	mock "github.com/stretchr/testify/mock"

	raft "github.com/niukuo/ragit/raft"
)

// Restorer is an autogenerated mock type for the Restorer type
type Restorer struct {
	mock.Mock
}

// Restore provides a mock function with given fields: cfg
func (_m *Restorer) Restore(cfg bdb.RestoreConfig) error {
	ret := _m.Called(cfg)

	var r0 error
	if rf, ok := ret.Get(0).(func(bdb.RestoreConfig) error); ok {
		r0 = rf(cfg)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Status provides a mock function with given fields: dbPath
func (_m *Restorer) Status(dbPath string) (*raft.InitialState, error) {
	ret := _m.Called(dbPath)

	var r0 *raft.InitialState
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (*raft.InitialState, error)); ok {
		return rf(dbPath)
	}
	if rf, ok := ret.Get(0).(func(string) *raft.InitialState); ok {
		r0 = rf(dbPath)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*raft.InitialState)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(dbPath)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewRestorer interface {
	mock.TestingT
	Cleanup(func())
}

// NewRestorer creates a new instance of Restorer. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewRestorer(t mockConstructorTestingTNewRestorer) *Restorer {
	mock := &Restorer{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
