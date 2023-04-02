package ugm

import (
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"testing"
)

func getQTMaxResource(qt *QueueTracker) map[string]*resources.Resource {
	resources := make(map[string]*resources.Resource)
	usage := qt.getResourceUsageDAOInfo("")
	return InternalGetMaxResource(usage, resources)
}

func internalGetMaxRunningApplications(usage *dao.ResourceUsageDAOInfo, resources map[string]uint64) map[string]uint64 {
	resources[usage.QueuePath] = usage.MaxRunningApplications
	if len(usage.Children) > 0 {
		for _, resourceUsage := range usage.Children {
			internalGetMaxRunningApplications(resourceUsage, resources)
		}
	}
	return resources
}

func TestQTSetMaxApps(t *testing.T) {
	// Queue setup:
	// root->parent->child1->child12
	// root->parent->child2
	// root->parent->child12 (similar name like above leaf queue, but it is being treated differently as similar names are allowed)
	limitTracker := newLimitTracker("root", User)
	limitTracker.SetMaxApplications(100, queuePath1, User)
	limitTracker.SetMaxApplications(1000, queuePath2, User)

}

func TestQTSetMaxResource(t *testing.T) {
	// Queue setup:
	// root->parent->child1->child12
	// root->parent->child2
	// root->parent->child12 (similar name like above leaf queue, but it is being treated differently as similar names are allowed)

	usage1, _ := resources.NewResourceFromConf(map[string]string{"mem": "70M", "vcore": "70"})
	limitTracker := newLimitTracker("root", User)
	limitTracker.SetMaxResources(usage1, queuePath1, User)
	limitTracker.SetMaxResources(usage1, queuePath2, User)
}
