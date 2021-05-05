package firestore

import (
	"cloud.google.com/go/firestore"
	// "google.golang.org/api/option"
)

type FireDBLayer struct {
	client *firestore.Client

	breakouts map[string][]string
	decoders map[string] func([]byte) (interface {}, error)
}
