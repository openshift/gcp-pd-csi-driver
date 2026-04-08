package k8sclient

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// nodeGetStartupInterval and nodeGetStartupTimeout bound how long startup paths
// (main process before the CSI socket exists) wait for the apiserver when cluster
// networking is not ready yet (e.g. CNI after a node reboot).
const (
	nodeGetStartupInterval = 5 * time.Second
	nodeGetStartupTimeout  = 10 * time.Minute
)

func newInClusterKubeClient() (*kubernetes.Clientset, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

// GetNodeWithRetry uses a short backoff for runtime CSI RPC paths (e.g. NodeGetInfo)
// so callers are not blocked for long on transient errors.
func GetNodeWithRetry(ctx context.Context, nodeName string) (*v1.Node, error) {
	if nodeName == "" {
		return nil, fmt.Errorf("node name is empty")
	}
	kubeClient, err := newInClusterKubeClient()
	if err != nil {
		return nil, err
	}
	return getNodeWithRetry(ctx, kubeClient, nodeName)
}

// GetNodeWithStartupRetry polls until the node can be read from the API server or
// nodeGetStartupTimeout elapses. Use during process startup before the gRPC server
// is listening so the node-driver-registrar does not time out on the socket.
func GetNodeWithStartupRetry(ctx context.Context, nodeName string) (*v1.Node, error) {
	if nodeName == "" {
		return nil, fmt.Errorf("node name is empty")
	}
	kubeClient, err := newInClusterKubeClient()
	if err != nil {
		return nil, err
	}
	return getNodeWithStartupPoll(ctx, kubeClient, nodeName)
}

func getNodeWithRetry(ctx context.Context, kubeClient *kubernetes.Clientset, nodeName string) (*v1.Node, error) {
	var nodeObj *v1.Node
	backoff := wait.Backoff{
		Duration: 1 * time.Second,
		Factor:   2.0,
		Steps:    5,
	}
	err := wait.ExponentialBackoffWithContext(ctx, backoff, func(_ context.Context) (bool, error) {
		node, err := kubeClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			klog.Warningf("Error getting node %s: %v, retrying...\n", nodeName, err)
			return false, nil
		}
		nodeObj = node
		klog.V(4).Infof("Successfully retrieved node info %s\n", nodeName)
		return true, nil
	})

	if err != nil {
		klog.Errorf("Failed to get node %s after retries: %v\n", nodeName, err)
	}
	return nodeObj, err
}

func getNodeWithStartupPoll(ctx context.Context, kubeClient *kubernetes.Clientset, nodeName string) (*v1.Node, error) {
	var nodeObj *v1.Node
	err := wait.PollUntilContextTimeout(ctx, nodeGetStartupInterval, nodeGetStartupTimeout, true, func(ctx context.Context) (bool, error) {
		node, err := kubeClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			klog.Warningf("Error getting node %s: %v, retrying until API server is reachable...", nodeName, err)
			return false, nil
		}
		nodeObj = node
		klog.V(4).Infof("Successfully retrieved node info %s\n", nodeName)
		return true, nil
	})
	if err != nil {
		klog.Errorf("Failed to get node %s after extended startup retries: %v\n", nodeName, err)
		return nil, err
	}
	return nodeObj, nil
}
