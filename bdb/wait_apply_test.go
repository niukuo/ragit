package bdb

import (
	"testing"

	"github.com/niukuo/ragit/logging"
	"github.com/stretchr/testify/assert"
)

func TestApplyDataIndex(t *testing.T) {
	s := assert.New(t)

	w := newWaitApplyRequests(10, logging.GetLogger("test"))
	{
		w.appliedIndex = 5
		w.applyDataIndex(3)
		s.EqualValues(5, w.appliedIndex)
	}

	{
		w.appliedIndex = 5
		w.applyDataIndex(0)
		s.EqualValues(0, w.appliedIndex)
	}

	{
		w.appliedIndex = 6
		w.applyDataIndex(10)
		s.EqualValues(10, w.appliedIndex)
		s.EqualValues(10, w.confIndexLeft)
		s.EqualValues(10, w.confIndexRight)
	}
}

func TestApplyConfIndex(t *testing.T) {
	s := assert.New(t)

	w := newWaitApplyRequests(10, logging.GetLogger("test"))
	{
		w.confIndexLeft = 5
		w.confIndexRight = 7
		w.applyConfIndex(4)
		s.EqualValues(5, w.confIndexLeft)
		s.EqualValues(7, w.confIndexRight)
	}

	{
		w.confIndexLeft = 5
		w.confIndexRight = 7
		w.applyConfIndex(0)
		s.EqualValues(0, w.confIndexLeft)
		s.EqualValues(0, w.confIndexRight)
	}

	{
		w.confIndexLeft = 5
		w.confIndexRight = 7
		w.applyConfIndex(8)
		s.EqualValues(5, w.confIndexLeft)
		s.EqualValues(8, w.confIndexRight)
	}

	{
		w.confIndexLeft = 5
		w.confIndexRight = 7
		w.appliedIndex = 4
		w.applyConfIndex(8)
		s.EqualValues(8, w.appliedIndex)
		s.EqualValues(8, w.confIndexLeft)
		s.EqualValues(8, w.confIndexRight)
	}
}

func TestAppliedIndexChase_nolock(t *testing.T) {
	s := assert.New(t)

	w := newWaitApplyRequests(10, logging.GetLogger("test"))
	{
		w.appliedIndex = 5
		w.confIndexLeft = 6
		w.confIndexRight = 10
		s.True(w.appliedIndexChase_nolock())
		s.EqualValues(10, w.appliedIndex)
		s.EqualValues(10, w.confIndexLeft)
		s.EqualValues(10, w.confIndexRight)
	}

	{
		w.appliedIndex = 4
		w.confIndexLeft = 6
		w.confIndexRight = 10
		s.False(w.appliedIndexChase_nolock())
		s.EqualValues(4, w.appliedIndex)
		s.EqualValues(6, w.confIndexLeft)
		s.EqualValues(10, w.confIndexRight)
	}

	{
		w.appliedIndex = 11
		w.confIndexLeft = 6
		w.confIndexRight = 10
		s.False(w.appliedIndexChase_nolock())
		s.EqualValues(11, w.appliedIndex)
		s.EqualValues(11, w.confIndexLeft)
		s.EqualValues(11, w.confIndexRight)
	}
}
