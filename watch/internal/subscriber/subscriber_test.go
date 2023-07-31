package subscriber

import (
	"fmt"
	"reflect"
)

// ComparableSubscriberT is a type constraint.
// It declares all types that can be returned by newComparableSubscriber().
// If additional subscriber types are added, they should be included here.
type ComparableSubscriberT interface {
	ComparableSubscriber | ComparableGRPCSubscriber
}

// newComparableSubscriber is to be used ONLY for testing (notably TestNewSubscribersFromJson).
// This uses reflection to determine what fields are shared between a subscriber and comparable subscriber type.
// * copyFromSubscriber should be an initialized struct representing the actual subscriber type (for example `&BaseSubscriber{}`).
// * toComparableType should be the desired comparable subscriber type to return (for example `&ComparableBaseSubscriber{}`).
// It returns an initialized toComparableType based on the provided copyFromSubscriber.
func newComparableSubscriber[T ComparableSubscriberT](copyFromSubscriber Interface, toComparableType *T) *T {

	src := reflect.ValueOf(copyFromSubscriber).Elem()
	dest := reflect.ValueOf(toComparableType).Elem()

	comparableSubscriber := reflect.New(dest.Type())

	for i := 0; i < src.NumField(); i++ {
		srcField := src.Type().Field(i)
		destField, ok := comparableSubscriber.Elem().Type().FieldByName(srcField.Name)
		if ok {
			if destField.Type == srcField.Type {
				comparableSubscriber.Elem().FieldByName(srcField.Name).Set(src.Field(i))
			} else {
				fmt.Printf("test failed because there is a field %s that exists in the subscriber type and comparable subscriber but the field types do not match (%s != %s)\n", srcField.Name, destField.Type, srcField.Type)
				return nil
			}
		}
	}

	newComparableSubscriber, ok := comparableSubscriber.Interface().(*T)
	if !ok {
		fmt.Printf("test failed because we were unable to cast the generated comparable subscriber to the inferred comparable type")
		return nil
	}

	return newComparableSubscriber
}
