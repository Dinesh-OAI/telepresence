package mutator

import (
	"context"
	"encoding/json/v2"
	"testing"

	argofake "github.com/datawire/argo-rollouts-go-client/pkg/client/clientset/versioned/fake"
	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	"github.com/telepresenceio/clog/testutil"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/agentmap"
	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/informer"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

const restoredAgentImage = "registry.example/traffic-agent:test"

func restoredAgentFixture(t *testing.T) (context.Context, *fake.Clientset, *configWatcher, *core.Pod, *agentconfig.Sidecar, *agentmap.GeneratorConfig) {
	t.Helper()
	env := &managerutil.Env{
		ManagerNamespace:           "default",
		ServerPort:                 8081,
		AgentRegistry:              "registry.example",
		AgentImageName:             "traffic-agent",
		AgentImageTag:              "test",
		AgentInjectPolicy:          agentconfig.OnDemand,
		AgentInjectorMutationAware: true,
		EnabledWorkloadKinds:       k8sapi.Kinds{k8sapi.DeploymentKind},
		AgentImagePullPolicy:       string(core.PullAlways),
		AgentImagePullSecrets:      []core.LocalObjectReference{{Name: "test-pull-secret"}},
		AgentInitResources:         &core.ResourceRequirements{},
		AgentSecurityContext:       &core.SecurityContext{RunAsUser: ptr.To(int64(7777)), RunAsGroup: ptr.To(int64(7778))},
		AgentInitSecurityContext:   &core.SecurityContext{RunAsUser: ptr.To(int64(8888))},
	}
	ctx := managerutil.WithEnv(testutil.NewContext(t, false), env)
	ctx = managerutil.WithResolvedAgentImageRetriever(ctx, managerutil.ImageFromEnv(restoredAgentImage))
	gc, err := env.GeneratorConfig(restoredAgentImage)
	require.NoError(t, err)
	sc := &agentconfig.Sidecar{
		AgentName: "example-worker", Namespace: "default", WorkloadName: "example-worker", WorkloadKind: k8sapi.DeploymentKind,
		AgentImage:  restoredAgentImage,
		ManagerHost: "traffic-manager.default", ManagerPort: 8081,
		PullPolicy: gc.PullPolicy, PullSecrets: gc.PullSecrets,
		InitResources: gc.InitResources, SecurityContext: gc.SecurityContext, InitSecurityContext: gc.InitSecurityContext,
		Containers: []*agentconfig.Container{{
			Name: "app", EnvPrefix: "A_", Replace: agentconfig.ReplacePolicyInactive,
			Intercepts: []*agentconfig.Intercept{{ContainerPortName: "http", ContainerPort: 8080, AgentPort: 9900, InactivePort: 9080, Protocol: types.ProtoTCP}},
		}},
	}
	tight, err := agentconfig.MarshalTight(sc)
	require.NoError(t, err)
	template := core.PodTemplateSpec{Spec: core.PodSpec{Containers: []core.Container{{
		Name: "app", Image: "registry.example/app:test", Ports: []core.ContainerPort{{Name: "http", ContainerPort: 8080}},
	}}}}
	deployment := &apps.Deployment{ObjectMeta: meta.ObjectMeta{Name: sc.WorkloadName, Namespace: sc.Namespace}, Spec: apps.DeploymentSpec{Template: template}}
	pod := &core.Pod{
		TypeMeta: meta.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: meta.ObjectMeta{
			Name: "example-worker-old", Namespace: sc.Namespace,
			Labels:      map[string]string{agentconfig.WorkloadNameLabel: sc.WorkloadName, agentconfig.WorkloadKindLabel: string(sc.WorkloadKind)},
			Annotations: map[string]string{annotation.Config: tight, annotation.InjectTrafficAgent: "enabled"},
		},
		Spec:   template.Spec,
		Status: core.PodStatus{Phase: core.PodRunning},
	}
	pod.Spec.Containers = append(pod.Spec.Containers, core.Container{Name: agentconfig.ContainerName, Image: restoredAgentImage})
	client := fake.NewSimpleClientset(deployment, pod)
	ctx = k8sapi.WithJoinedClientSetInterface(ctx, client, argofake.NewSimpleClientset())
	ctx = informer.WithFactory(ctx, "")
	factory := informer.GetK8sFactory(ctx, "")
	require.NoError(t, factory.Apps().V1().Deployments().Informer().GetStore().Add(deployment.DeepCopy()))
	require.NoError(t, factory.Core().V1().Pods().Informer().GetStore().Add(pod.DeepCopy()))
	newPod := pod.DeepCopy()
	newPod.Name = "example-worker-new"
	newPod.Spec = template.Spec
	newPod.Annotations = map[string]string{}
	return ctx, client, NewWatcher().(*configWatcher), newPod, sc, gc
}

func agentFromRestorePatch(t *testing.T, patch PatchOps) *core.Container {
	t.Helper()
	for _, op := range patch {
		if op.Path != "/spec/containers/-" {
			continue
		}
		data, err := json.Marshal(op.Value)
		require.NoError(t, err)
		var container core.Container
		require.NoError(t, json.Unmarshal(data, &container))
		if container.Name == agentconfig.ContainerName {
			return &container
		}
	}
	t.Fatal("admission patch did not include a traffic-agent container")
	return nil
}

func TestStartupRestoresAgentContainerConfig(t *testing.T) {
	ctx, client, cw, _, original, gc := restoredAgentFixture(t)
	require.False(t, cw.configured.Load())
	require.NoError(t, cw.regenerateAgentConfigs(ctx, original.Namespace, gc))
	restored := cw.Get(original.AgentName, original.Namespace)
	require.NotNil(t, restored)
	require.Equal(t, original, restored)
	require.Empty(t, client.Actions(), "startup restoration must not write or evict pods")
}

func TestRestoredAgentAdmissionPreservesConfig(t *testing.T) {
	for _, startup := range []bool{true, false} {
		for _, enabled := range []bool{true, false} {
			name := "stale-cache/on-demand"
			if startup {
				name = "startup/on-demand"
			}
			if enabled {
				name += "/enabled"
			}
			t.Run(name, func(t *testing.T) {
				ctx, client, cw, pod, original, gc := restoredAgentFixture(t)
				if startup {
					require.NoError(t, cw.regenerateAgentConfigs(ctx, original.Namespace, gc))
					cw.SetConfigured()
				} else {
					tight, err := agentconfig.MarshalTight(original)
					require.NoError(t, err)
					restored, err := agentconfig.UnmarshalJSON(tight)
					require.NoError(t, err)
					cw.Store(restored)
				}
				if enabled {
					pod.Annotations[annotation.InjectTrafficAgent] = "enabled"
				}
				cached := cw.Get(original.AgentName, original.Namespace)
				before := cached.Clone()
				injector := agentInjector{agentConfigs: cw}
				patch, err := injector.Inject(ctx, toAdmissionRequest(podResource, pod))
				require.NoError(t, err)
				agent := agentFromRestorePatch(t, patch)
				require.Equal(t, restoredAgentImage, agent.Image)
				require.Equal(t, core.PullAlways, agent.ImagePullPolicy)
				require.Equal(t, original.SecurityContext, agent.SecurityContext)
				require.Equal(t, before, cached, "admission must not mutate the shared cache")
				require.Equal(t, original.Containers, cached.Containers, "selected intercept and inactive ports must remain intact")
				require.Empty(t, client.Actions(), "admission uses fake cached data without writes")
			})
		}
	}
}

func TestRestoredAgentAdmissionPreservesCompleteConfig(t *testing.T) {
	ctx, client, cw, pod, original, _ := restoredAgentFixture(t)
	original.AgentImage = "registry.example/custom-agent:pinned"
	original.PullPolicy = string(core.PullNever)
	original.SecurityContext = &core.SecurityContext{RunAsUser: ptr.To(int64(9999)), RunAsGroup: ptr.To(int64(9998))}
	cw.Store(original)
	before := original.Clone()
	injector := agentInjector{agentConfigs: cw}
	patch, err := injector.Inject(ctx, toAdmissionRequest(podResource, pod))
	require.NoError(t, err)
	agent := agentFromRestorePatch(t, patch)
	require.Equal(t, original.AgentImage, agent.Image)
	require.Equal(t, core.PullNever, agent.ImagePullPolicy)
	require.Equal(t, original.SecurityContext, agent.SecurityContext)
	require.Equal(t, before, original)
	require.Empty(t, client.Actions())
}

func TestRestoredAgentAdmissionRestoresInitFields(t *testing.T) {
	ctx, client, cw, pod, original, _ := restoredAgentFixture(t)
	env := managerutil.GetEnv(ctx)
	env.AgentInitContainerEnabled = true
	env.AgentInitResources = &core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("100m")}}
	original.InitResources = env.AgentInitResources
	original.Containers[0].Replace = agentconfig.ReplacePolicyIntercept
	original.Containers[0].Intercepts[0].TargetPortNumeric = true
	tight, err := agentconfig.MarshalTight(original)
	require.NoError(t, err)
	cached, err := agentconfig.UnmarshalJSON(tight)
	require.NoError(t, err)
	cw.Store(cached)
	before := cached.Clone()
	injector := agentInjector{agentConfigs: cw}
	patch, err := injector.Inject(ctx, toAdmissionRequest(podResource, pod))
	require.NoError(t, err)
	require.Equal(t, restoredAgentImage, agentFromRestorePatch(t, patch).Image)
	var initContainers []core.Container
	var pullSecrets []core.LocalObjectReference
	for _, op := range patch {
		if op.Path != "/spec/initContainers" && op.Path != "/spec/imagePullSecrets" {
			continue
		}
		data, err := json.Marshal(op.Value)
		require.NoError(t, err)
		if op.Path == "/spec/initContainers" {
			require.NoError(t, json.Unmarshal(data, &initContainers))
		} else {
			require.NoError(t, json.Unmarshal(data, &pullSecrets))
		}
	}
	require.Len(t, initContainers, 1)
	require.Equal(t, agentconfig.InitContainerName, initContainers[0].Name)
	require.Equal(t, restoredAgentImage, initContainers[0].Image)
	require.Equal(t, *original.InitResources, initContainers[0].Resources)
	require.Equal(t, original.InitSecurityContext, initContainers[0].SecurityContext)
	require.Equal(t, original.PullSecrets, pullSecrets)
	require.Equal(t, before, cached)
	require.Empty(t, client.Actions())
}

func TestRestoredAgentAdmissionHonorsSkipPolicies(t *testing.T) {
	for _, policy := range []string{"manual", "disabled", "no-manager-image"} {
		t.Run(policy, func(t *testing.T) {
			ctx, client, cw, pod, original, _ := restoredAgentFixture(t)
			tight, err := agentconfig.MarshalTight(original)
			require.NoError(t, err)
			restored, err := agentconfig.UnmarshalJSON(tight)
			require.NoError(t, err)
			switch policy {
			case "manual":
				restored.Manual = true
			case "disabled":
				pod.Annotations[annotation.InjectTrafficAgent] = "disabled"
			case "no-manager-image":
				ctx = managerutil.WithResolvedAgentImageRetriever(ctx, managerutil.ImageFromEnv(""))
			}
			cw.Store(restored)
			before := restored.Clone()
			injector := agentInjector{agentConfigs: cw}
			patch, err := injector.Inject(ctx, toAdmissionRequest(podResource, pod))
			require.NoError(t, err)
			require.Empty(t, patch)
			require.Equal(t, before, restored)
			require.Empty(t, client.Actions())
		})
	}
}
