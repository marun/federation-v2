/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dispatch

import (
	"context"

	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog"

	"sigs.k8s.io/kubefed/pkg/client/generic"
	"sigs.k8s.io/kubefed/pkg/controller/sync/status"
	"sigs.k8s.io/kubefed/pkg/controller/util"
)

const eventTemplate = "%s %s %q in cluster %q"

// UnmanagedDispatcher dispatches operations to member clusters for
// resources that are no longer managed by a federated resource.
type UnmanagedDispatcher interface {
	OperationDispatcher

	Delete(clusterName string)
	RemoveManagedLabel(clusterName string, clusterObj *unstructured.Unstructured)
}

type unmanagedDispatcherImpl struct {
	dispatcher *operationDispatcherImpl

	targetGVK  schema.GroupVersionKind
	targetName util.QualifiedName

	recorder dispatchRecorder

	scaleTesting bool
}

func NewUnmanagedDispatcher(clientAccessor clientAccessorFunc, targetGVK schema.GroupVersionKind, targetName util.QualifiedName, scaleTesting bool) UnmanagedDispatcher {
	opDispatcher := newOperationDispatcher(clientAccessor, nil)
	unmanagedDispatcher := newUnmanagedDispatcher(opDispatcher, nil, targetGVK, targetName)
	unmanagedDispatcher.scaleTesting = scaleTesting
	return unmanagedDispatcher
}

func newUnmanagedDispatcher(dispatcher *operationDispatcherImpl, recorder dispatchRecorder, targetGVK schema.GroupVersionKind, targetName util.QualifiedName) *unmanagedDispatcherImpl {
	return &unmanagedDispatcherImpl{
		dispatcher: dispatcher,
		targetGVK:  targetGVK,
		targetName: targetName,
		recorder:   recorder,
	}
}

func (d *unmanagedDispatcherImpl) Wait() (bool, error) {
	return d.dispatcher.Wait()
}

func (d *unmanagedDispatcherImpl) Delete(clusterName string) {
	d.dispatcher.incrementOperationsInitiated()
	const op = "delete"
	const opContinuous = "Deleting"
	go d.dispatcher.clusterOperation(clusterName, op, func(client generic.Client) util.ReconciliationStatus {
		targetName := d.targetName
		if d.scaleTesting {
			targetName = util.QualifiedName{
				Namespace: clusterName,
				Name:      targetName.Name,
			}
		}

		if d.recorder == nil {
			klog.V(2).Infof(eventTemplate, opContinuous, d.targetGVK.Kind, targetName, clusterName)
		} else {
			d.recorder.recordEvent(clusterName, op, opContinuous)
		}

		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(d.targetGVK)
		err := client.Delete(context.Background(), obj, targetName.Namespace, targetName.Name)
		if apierrors.IsNotFound(err) {
			err = nil
		}
		if err != nil {
			if d.recorder == nil {
				wrappedErr := d.wrapOperationError(err, clusterName, op)
				runtime.HandleError(wrappedErr)
			} else {
				d.recorder.recordOperationError(status.DeletionFailed, clusterName, op, err)
			}
			return util.StatusError
		}
		return util.StatusAllOK
	})
}

func (d *unmanagedDispatcherImpl) RemoveManagedLabel(clusterName string, clusterObj *unstructured.Unstructured) {
	d.dispatcher.incrementOperationsInitiated()
	const op = "remove managed label from"
	const opContinuous = "Removing managed label from"
	go d.dispatcher.clusterOperation(clusterName, op, func(client generic.Client) util.ReconciliationStatus {
		if d.recorder == nil {
			klog.V(2).Infof(eventTemplate, opContinuous, d.targetGVK.Kind, d.targetName, clusterName)
		} else {
			d.recorder.recordEvent(clusterName, op, opContinuous)
		}

		// Avoid mutating the resource in the informer cache
		updateObj := clusterObj.DeepCopy()

		util.RemoveManagedLabel(updateObj)

		err := client.Update(context.Background(), updateObj)
		if err != nil {
			if d.recorder == nil {
				wrappedErr := d.wrapOperationError(err, clusterName, op)
				runtime.HandleError(wrappedErr)
			} else {
				d.recorder.recordOperationError(status.LabelRemovalFailed, clusterName, op, err)
			}
			return util.StatusError
		}
		return util.StatusAllOK
	})
}

func (d *unmanagedDispatcherImpl) wrapOperationError(err error, clusterName, operation string) error {
	return wrapOperationError(err, operation, d.targetGVK.Kind, d.targetName.String(), clusterName)
}

func wrapOperationError(err error, operation, targetKind, targetName, clusterName string) error {
	return errors.Wrapf(err, "Failed to "+eventTemplate, operation, targetKind, targetName, clusterName)
}
