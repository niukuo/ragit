package raft

type RunnerFunc = func(stopC <-chan struct{}) error

type Runner interface {
	Stop()
	Done() <-chan struct{}
	Error() error
}

type runner struct {
	stopC chan<- struct{}
	doneC <-chan struct{}
	err   error
}

func StartRunner(fn RunnerFunc) Runner {
	stopC := make(chan struct{})
	doneC := make(chan struct{})

	r := &runner{
		stopC: stopC,
		doneC: doneC,
	}

	go func() {
		r.err = fn(stopC)
		close(doneC)
	}()

	return r
}

func (r *runner) Stop() {
	close(r.stopC)
}

func (r *runner) Done() <-chan struct{} {
	return r.doneC
}

// must call after done
func (r *runner) Error() error {
	return r.err
}
