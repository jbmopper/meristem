package listeneractivation

import "testing"

func TestValidActivationTransition(t *testing.T) {
	valid := map[[2]State]bool{
		{StateRequested, StateDispatching}: true,
		{StateFailed, StateDispatching}:    true,
		{StateAmbiguous, StateDispatching}: true,
		{StateDispatching, StateAccepted}:  true,
		{StateDispatching, StateCompleted}: true,
		{StateDispatching, StateFailed}:    true,
		{StateDispatching, StateAmbiguous}: true,
		{StateAccepted, StateCompleted}:    true,
		{StateAccepted, StateFailed}:       true,
		{StateAccepted, StateAmbiguous}:    true,
	}
	states := []State{
		StateRequested, StateDispatching, StateAccepted,
		StateCompleted, StateFailed, StateAmbiguous,
	}
	for _, from := range states {
		for _, to := range states {
			want := valid[[2]State{from, to}]
			if got := validActivationTransition(from, to); got != want {
				t.Errorf("validActivationTransition(%q, %q) = %t, want %t", from, to, got, want)
			}
		}
	}
}
