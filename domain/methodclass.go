package domain

// Method classes, by what a method costs a node to answer: a head, a chain id
// or a health check is light; a log scan, a contract call, a trace or a
// program-account scan is heavy; the rest is standard. A chain's QoS plugin
// says which of its methods is which (qos.MethodClassifier).
const (
	MethodClassLight    = "light"
	MethodClassStandard = "standard"
	MethodClassHeavy    = "heavy"
)
