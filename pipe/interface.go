package pipe

type broadcaster[T any] interface {
	FanOut(params Params) Piper[T]
	FanIn(params Params) Piper[T]
	Broadcast(params Params) Piper[T]
}

type streamer[T any] interface {
	OrDone(params Params) Piper[T]
	Tee(params Params) (Piper[T], Piper[T])
	Take(params Params) Piper[T]
}

type Piper[T any] interface {
	broadcaster[T]
	streamer[T]
	Out() <-chan T
}
