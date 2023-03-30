/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package ugm

import (
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
)

type trackingType int

const (
	None trackingType = iota
	User
	Group
)

type LimitTracker struct {
	trackerType      trackingType
	queueName        string
	maxResourceUsage *resources.Resource
	maxRunningApps   uint64

	childLimitTrackers map[string]*LimitTracker
}

func newRootLimitTracker(trackType trackingType) *LimitTracker {
	return newLimitTracker(configs.RootQueue, trackType)
}

func newLimitTracker(queueName string, trackingType trackingType) *LimitTracker {
	log.Logger().Debug("Creating limit tracker object for queue",
		zap.String("queue", queueName))
	limitTracker := &LimitTracker{
		trackerType:        trackingType,
		queueName:          queueName,
		maxResourceUsage:   resources.NewResource(),
		maxRunningApps:     0,
		childLimitTrackers: make(map[string]*LimitTracker),
	}
	return limitTracker
}

func (lt *LimitTracker) SetMaxApplications(maxApps uint64, queuePath string, trackingType trackingType) {
	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
	if childQueuePath != "" {
		if lt.childLimitTrackers[immediateChildQueueName] == nil {
			lt.childLimitTrackers[immediateChildQueueName] = newLimitTracker(immediateChildQueueName, trackingType)
		}
		lt.childLimitTrackers[immediateChildQueueName].SetMaxApplications(maxApps, childQueuePath, trackingType)
	} else {
		lt.maxRunningApps = maxApps
	}
}

func (lt *LimitTracker) SetMaxResources(maxResource *resources.Resource, queuePath string, trackingType trackingType) {
	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
	if childQueuePath != "" {
		if lt.childLimitTrackers[immediateChildQueueName] == nil {
			lt.childLimitTrackers[immediateChildQueueName] = newLimitTracker(immediateChildQueueName, trackingType)
		}
		lt.childLimitTrackers[immediateChildQueueName].SetMaxResources(maxResource, childQueuePath, trackingType)
	} else {
		lt.maxResourceUsage = maxResource.Clone()
	}
}
