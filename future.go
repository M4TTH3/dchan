package dchan

type Future struct {
	ch chan error
}

func newFuture() Future {
	return Future{
		ch: make(chan error, 1),
	}
}

// Wait blocks the future until it's complete and returns (possibly an error).
func (f Future) Wait() error {
	res := <-f.ch
	f.ch <- res // Pass if there's another wait call
	return res
}

// set sets the future to the given error.
//
// Note: this function should be only called once.
func (f Future) set(err error) {
	f.ch <- err
}
