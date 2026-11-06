INSTALL VIRTUALBOX
==================

https://download.virtualbox.org/virtualbox/7.1.6/VirtualBox-7.1.6-167084-Win.exe


INSTALL MINIKUBE CLI
====================

https://minikube.sigs.k8s.io/docs/start/?arch=%2Fwindows%2Fx86-64%2Fstable%2F.exe+download

INSTALL MINIKUBE CLUSTER
========================


User@MSI MINGW64 ~
Mon Apr 21 19:43:13
$ minikube start --vm-driver virtualbox --memory=4096     --cpus=4 --disk-size=40960     --no-vtx-check=true
* minikube v1.25.2 en Microsoft Windows 11 Home 10.0.26100 Build 26100
  - MINIKUBE_HOME=C:\Users\User\.minikube
* Using the virtualbox driver based on existing profile
! You cannot change the memory size for an existing minikube cluster. Please first delete the cluster.
! You cannot change the CPUs for an existing minikube cluster. Please first delete the cluster.
! You cannot change the disk size for an existing minikube cluster. Please first delete the cluster.
* Starting control plane node minikube in cluster minikube
* virtualbox "minikube" VM is missing, will recreate.
* Creando virtualbox VM (CPUs=2, Memory=6000MB, Disk=20000MB) ...
* Preparando Kubernetes v1.23.3 en Docker 20.10.12...
  - kubelet.housekeeping-interval=5m
  - Generando certificados y llaves
  - Iniciando plano de control
  - Configurando reglas RBAC...
  - Using image k8s.gcr.io/metrics-server/metrics-server:v0.4.2
  - Using image gcr.io/k8s-minikube/storage-provisioner:v5
  - Using image kubernetesui/dashboard:v2.3.1
  - Using image kubernetesui/metrics-scraper:v1.0.7
* Verifying Kubernetes components...
* Complementos habilitados: default-storageclass, storage-provisioner, metrics-server, dashboard
* Done! kubectl is now configured to use "minikube" cluster and "default" namespace by default

User@MSI MINGW64 ~
Mon Apr 21 19:45:57
$ minikube status
minikube
type: Control Plane
host: Running
kubelet: Running
apiserver: Running
kubeconfig: Configured


User@MSI MINGW64 ~
Mon Apr 21 19:48:01
$ ssh docker@192.168.59.106
The authenticity of host '192.168.59.106 (192.168.59.106)' can't be established.
ED25519 key fingerprint is SHA256:kGoX/Yjh70+NH+UemLOqNsA/4PO5BUw0H2VsYn9V68Y.
This key is not known by any other names.
Are you sure you want to continue connecting (yes/no/[fingerprint])? yes
Warning: Permanently added '192.168.59.106' (ED25519) to the list of known hosts.
docker@192.168.59.106's password: tcuser
                         _             _
            _         _ ( )           ( )
  ___ ___  (_)  ___  (_)| |/')  _   _ | |_      __
/' _ ` _ `\| |/' _ `\| || , <  ( ) ( )| '_`\  /'__`\
| ( ) ( ) || || ( ) || || |\`\ | (_) || |_) )(  ___/
(_) (_) (_)(_)(_) (_)(_)(_) (_)`\___/'(_,__/'`\____)

$


INSTALL STRIMZI
===============

https://strimzi.io/quickstarts/


* Install kubectl

	$ curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
	  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
									 Dload  Upload   Total   Spent    Left  Speed
	100   138  100   138    0     0    474      0 --:--:-- --:--:-- --:--:--   475
	  8 54.6M    8 4830k    0     0   272k      0  0:03:25  0:00:17  0:03:08  247k


$ mkdir -p $HOME/.kube
$ sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
$ sudo chown $(id -u):$(id -g) $HOME/.kube/config
$
$
$
$
$ kubectl get po
No resources found in default namespace.


$ kubectl create namespace kafka
namespace/kafka created
$
$ kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
customresourcedefinition.apiextensions.k8s.io/kafkamirrormaker2s.kafka.strimzi.io created
customresourcedefinition.apiextensions.k8s.io/strimzipodsets.core.strimzi.io created
customresourcedefinition.apiextensions.k8s.io/kafkanodepools.kafka.strimzi.io created
customresourcedefinition.apiextensions.k8s.io/kafkamirrormakers.kafka.strimzi.io created
customresourcedefinition.apiextensions.k8s.io/kafkas.kafka.strimzi.io created
customresourcedefinition.apiextensions.k8s.io/kafkabridges.kafka.strimzi.io created
rolebinding.rbac.authorization.k8s.io/strimzi-cluster-operator-leader-election created
clusterrole.rbac.authorization.k8s.io/strimzi-entity-operator created
clusterrole.rbac.authorization.k8s.io/strimzi-cluster-operator-namespaced created
clusterrolebinding.rbac.authorization.k8s.io/strimzi-cluster-operator-kafka-client-delegation created
rolebinding.rbac.authorization.k8s.io/strimzi-cluster-operator-watched created
serviceaccount/strimzi-cluster-operator created
clusterrole.rbac.authorization.k8s.io/strimzi-cluster-operator-global created
customresourcedefinition.apiextensions.k8s.io/kafkaconnectors.kafka.strimzi.io created
rolebinding.rbac.authorization.k8s.io/strimzi-cluster-operator created
clusterrolebinding.rbac.authorization.k8s.io/strimzi-cluster-operator created
rolebinding.rbac.authorization.k8s.io/strimzi-cluster-operator-entity-operator-delegation created
clusterrole.rbac.authorization.k8s.io/strimzi-kafka-client created
clusterrole.rbac.authorization.k8s.io/strimzi-cluster-operator-leader-election created
clusterrole.rbac.authorization.k8s.io/strimzi-kafka-broker created
deployment.apps/strimzi-cluster-operator created
clusterrole.rbac.authorization.k8s.io/strimzi-cluster-operator-watched created
configmap/strimzi-cluster-operator created
customresourcedefinition.apiextensions.k8s.io/kafkaconnects.kafka.strimzi.io created
clusterrolebinding.rbac.authorization.k8s.io/strimzi-cluster-operator-kafka-broker-delegation created
customresourcedefinition.apiextensions.k8s.io/kafkarebalances.kafka.strimzi.io created
customresourcedefinition.apiextensions.k8s.io/kafkatopics.kafka.strimzi.io created
customresourcedefinition.apiextensions.k8s.io/kafkausers.kafka.strimzi.io created
$

$ kubectl get pod -n kafka --watch
NAME                                        READY   STATUS              RESTARTS   AGE
strimzi-cluster-operator-5bb458798b-lnzxq   0/1     ContainerCreating   0          34s


$ kubectl -n kafka get events
LAST SEEN   TYPE     REASON              OBJECT                                           MESSAGE
4m52s       Normal   Scheduled           pod/strimzi-cluster-operator-5bb458798b-lnzxq    Successfully assigned kafka/strimzi-cluster-operator-5bb458798b-lnzxq to minikube
4m51s       Normal   Pulling             pod/strimzi-cluster-operator-5bb458798b-lnzxq    Pulling image "quay.io/strimzi/operator:0.45.0"
4m52s       Normal   SuccessfulCreate    replicaset/strimzi-cluster-operator-5bb458798b   Created pod: strimzi-cluster-operator-5bb458798b-lnzxq
4m52s       Normal   ScalingReplicaSet   deployment/strimzi-cluster-operator              Scaled up replica set strimzi-cluster-operator-5bb458798b to 1


$ kubectl get all --all-namespaces
NAMESPACE              NAME                                            READY   STATUS              RESTARTS      AGE
kafka                  pod/strimzi-cluster-operator-5bb458798b-lnzxq   0/1     ContainerCreating   0             8m39s
kube-system            pod/coredns-64897985d-stkts                     1/1     Running             0             26m
kube-system            pod/etcd-minikube                               1/1     Running             0             26m
kube-system            pod/kube-apiserver-minikube                     1/1     Running             0             26m
kube-system            pod/kube-controller-manager-minikube            1/1     Running             0             26m
kube-system            pod/kube-proxy-khw4w                            1/1     Running             0             26m
kube-system            pod/kube-scheduler-minikube                     1/1     Running             0             26m
kube-system            pod/metrics-server-6b76bd68b6-jqkrr             1/1     Running             0             26m
kube-system            pod/storage-provisioner                         1/1     Running             1 (25m ago)   26m
kubernetes-dashboard   pod/dashboard-metrics-scraper-58549894f-zcwvp   1/1     Running             0             26m
kubernetes-dashboard   pod/kubernetes-dashboard-ccd587f44-k9xsq        1/1     Running             0             26m

NAMESPACE              NAME                                TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)                  AGE
default                service/kubernetes                  ClusterIP   10.96.0.1       <none>        443/TCP                  26m
kube-system            service/kube-dns                    ClusterIP   10.96.0.10      <none>        53/UDP,53/TCP,9153/TCP   26m
kube-system            service/metrics-server              ClusterIP   10.103.16.182   <none>        443/TCP                  26m
kubernetes-dashboard   service/dashboard-metrics-scraper   ClusterIP   10.109.95.17    <none>        8000/TCP                 26m
kubernetes-dashboard   service/kubernetes-dashboard        ClusterIP   10.98.208.116   <none>        80/TCP                   26m

NAMESPACE     NAME                        DESIRED   CURRENT   READY   UP-TO-DATE   AVAILABLE   NODE SELECTOR            AGE
kube-system   daemonset.apps/kube-proxy   1         1         1       1            1           kubernetes.io/os=linux   26m

NAMESPACE              NAME                                        READY   UP-TO-DATE   AVAILABLE   AGE
kafka                  deployment.apps/strimzi-cluster-operator    0/1     1            0           8m39s
kube-system            deployment.apps/coredns                     1/1     1            1           26m
kube-system            deployment.apps/metrics-server              1/1     1            1           26m
kubernetes-dashboard   deployment.apps/dashboard-metrics-scraper   1/1     1            1           26m
kubernetes-dashboard   deployment.apps/kubernetes-dashboard        1/1     1            1           26m

NAMESPACE              NAME                                                  DESIRED   CURRENT   READY   AGE
kafka                  replicaset.apps/strimzi-cluster-operator-5bb458798b   1         1         0       8m39s
kube-system            replicaset.apps/coredns-64897985d                     1         1         1       26m
kube-system            replicaset.apps/metrics-server-6b76bd68b6             1         1         1       26m
kubernetes-dashboard   replicaset.apps/dashboard-metrics-scraper-58549894f   1         1         1       26m
kubernetes-dashboard   replicaset.apps/kubernetes-dashboard-ccd587f44        1         1         1       26m


$ kubectl top nodes
NAME       CPU(cores)   CPU(%)   MEMORY(bytes)   MEMORY(%)
minikube   193m         9%       1515Mi          26%


$ kubectl -n kafka describe pods
Name:             strimzi-cluster-operator-5bb458798b-lnzxq
Namespace:        kafka
Priority:         0
Service Account:  strimzi-cluster-operator
Node:             minikube/192.168.59.106
Start Time:       Mon, 21 Apr 2025 18:03:43 +0000
Labels:           name=strimzi-cluster-operator
                  pod-template-hash=5bb458798b
                  strimzi.io/kind=cluster-operator
Annotations:      <none>
Status:           Running
IP:               172.17.0.6
IPs:
  IP:           172.17.0.6
Controlled By:  ReplicaSet/strimzi-cluster-operator-5bb458798b
Containers:
  strimzi-cluster-operator:
    Container ID:  docker://d8f70d631f7faa39cc8174a95ab7c2d7e6ef80c94c1ea6f9a67c708f23f7e7f3
    Image:         quay.io/strimzi/operator:0.45.0
    Image ID:      docker-pullable://quay.io/strimzi/operator@sha256:328829a67886159943a9d9f6a2e6ca1cfffadceea48eb53451395b7e795ec17e
    Port:          8080/TCP
    Host Port:     0/TCP
    Args:
      /opt/strimzi/bin/cluster_operator_run.sh
    State:          Running
      Started:      Mon, 21 Apr 2025 18:16:29 +0000
    Ready:          True
    Restart Count:  0
    Limits:
      cpu:     1
      memory:  384Mi
    Requests:
      cpu:      200m
      memory:   384Mi
    Liveness:   http-get http://:http/healthy delay=10s timeout=1s period=30s #success=1 #failure=3
    Readiness:  http-get http://:http/ready delay=10s timeout=1s period=30s #success=1 #failure=3
    Environment:
      STRIMZI_NAMESPACE:                        kafka (v1:metadata.namespace)
      STRIMZI_FULL_RECONCILIATION_INTERVAL_MS:  120000
      STRIMZI_OPERATION_TIMEOUT_MS:             300000
      STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE:     quay.io/strimzi/kafka:0.45.0-kafka-3.9.0
      STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE:     quay.io/strimzi/kafka:0.45.0-kafka-3.9.0
      STRIMZI_KAFKA_IMAGES:                     3.8.0=quay.io/strimzi/kafka:0.45.0-kafka-3.8.0
                                                3.8.1=quay.io/strimzi/kafka:0.45.0-kafka-3.8.1
                                                3.9.0=quay.io/strimzi/kafka:0.45.0-kafka-3.9.0

      STRIMZI_KAFKA_CONNECT_IMAGES:             3.8.0=quay.io/strimzi/kafka:0.45.0-kafka-3.8.0
                                                3.8.1=quay.io/strimzi/kafka:0.45.0-kafka-3.8.1
                                                3.9.0=quay.io/strimzi/kafka:0.45.0-kafka-3.9.0

      STRIMZI_KAFKA_MIRROR_MAKER_IMAGES:        3.8.0=quay.io/strimzi/kafka:0.45.0-kafka-3.8.0
                                                3.8.1=quay.io/strimzi/kafka:0.45.0-kafka-3.8.1
                                                3.9.0=quay.io/strimzi/kafka:0.45.0-kafka-3.9.0

      STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES:      3.8.0=quay.io/strimzi/kafka:0.45.0-kafka-3.8.0
                                                3.8.1=quay.io/strimzi/kafka:0.45.0-kafka-3.8.1
                                                3.9.0=quay.io/strimzi/kafka:0.45.0-kafka-3.9.0

      STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE:     quay.io/strimzi/operator:0.45.0
      STRIMZI_DEFAULT_USER_OPERATOR_IMAGE:      quay.io/strimzi/operator:0.45.0
      STRIMZI_DEFAULT_KAFKA_INIT_IMAGE:         quay.io/strimzi/operator:0.45.0
      STRIMZI_DEFAULT_KAFKA_BRIDGE_IMAGE:       quay.io/strimzi/kafka-bridge:0.31.1
      STRIMZI_DEFAULT_KANIKO_EXECUTOR_IMAGE:    quay.io/strimzi/kaniko-executor:0.45.0
      STRIMZI_DEFAULT_MAVEN_BUILDER:            quay.io/strimzi/maven-builder:0.45.0
      STRIMZI_OPERATOR_NAMESPACE:               kafka (v1:metadata.namespace)
      STRIMZI_FEATURE_GATES:
      STRIMZI_LEADER_ELECTION_ENABLED:          true
      STRIMZI_LEADER_ELECTION_LEASE_NAME:       strimzi-cluster-operator
      STRIMZI_LEADER_ELECTION_LEASE_NAMESPACE:  kafka (v1:metadata.namespace)
      STRIMZI_LEADER_ELECTION_IDENTITY:         strimzi-cluster-operator-5bb458798b-lnzxq (v1:metadata.name)
    Mounts:
      /opt/strimzi/custom-config/ from co-config-volume (rw)
      /tmp from strimzi-tmp (rw)
      /var/run/secrets/kubernetes.io/serviceaccount from kube-api-access-mq4ww (ro)
Conditions:
  Type              Status
  Initialized       True
  Ready             True
  ContainersReady   True
  PodScheduled      True
Volumes:
  strimzi-tmp:
    Type:       EmptyDir (a temporary directory that shares a pod's lifetime)
    Medium:     Memory
    SizeLimit:  1Mi
  co-config-volume:
    Type:      ConfigMap (a volume populated by a ConfigMap)
    Name:      strimzi-cluster-operator
    Optional:  false
  kube-api-access-mq4ww:
    Type:                    Projected (a volume that contains injected data from multiple sources)
    TokenExpirationSeconds:  3607
    ConfigMapName:           kube-root-ca.crt
    ConfigMapOptional:       <nil>
    DownwardAPI:             true
QoS Class:                   Burstable
Node-Selectors:              <none>
Tolerations:                 node.kubernetes.io/not-ready:NoExecute op=Exists for 300s
                             node.kubernetes.io/unreachable:NoExecute op=Exists for 300s
Events:
  Type    Reason     Age   From               Message
  ----    ------     ----  ----               -------
  Normal  Scheduled  13m   default-scheduler  Successfully assigned kafka/strimzi-cluster-operator-5bb458798b-lnzxq to minikube
  Normal  Pulling    13m   kubelet            Pulling image "quay.io/strimzi/operator:0.45.0"
  Normal  Pulled     54s   kubelet            Successfully pulled image "quay.io/strimzi/operator:0.45.0" in 11m57.172729325s
  Normal  Created    53s   kubelet            Created container strimzi-cluster-operator
  Normal  Started    53s   kubelet            Started container strimzi-cluster-operator



$ kubectl -n kafka get all
NAME                                            READY   STATUS    RESTARTS   AGE
pod/strimzi-cluster-operator-5bb458798b-lnzxq   1/1     Running   0          14m

NAME                                       READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/strimzi-cluster-operator   1/1     1            1           14m

NAME                                                  DESIRED   CURRENT   READY   AGE
replicaset.apps/strimzi-cluster-operator-5bb458798b   1         1         1       14m




$ kubectl apply -f https://strimzi.io/examples/latest/kafka/kraft/kafka-single-node.yaml -n kafka 

$ kubectl apply -f https://strimzi.io/examples/latest/kafka/kraft/kafka-single-node.yaml -n kafka
kafkanodepool.kafka.strimzi.io/dual-role created
kafka.kafka.strimzi.io/my-cluster created

$ kubectl -n kafka get po
NAME                                         READY   STATUS    RESTARTS      AGE
my-cluster-dual-role-0                       1/1     Running   0             46s
my-cluster-entity-operator-8c5f878b7-g8hgg   2/2     Running   0             26s
strimzi-cluster-operator-5bb458798b-lnzxq    1/1     Running   1 (15m ago)   43m



$ kubectl -n kafka get po -o wide
NAME                                         READY   STATUS    RESTARTS      AGE    IP           NODE       NOMINATED NODE   READINESS GATES
my-cluster-dual-role-0                       1/1     Running   0             116s   172.17.0.7   minikube   <none>           <none>
my-cluster-entity-operator-8c5f878b7-g8hgg   2/2     Running   0             96s    172.17.0.8   minikube   <none>           <none>
strimzi-cluster-operator-5bb458798b-lnzxq    1/1     Running   1 (16m ago)   45m    172.17.0.6   minikube   <none>           <none>




$ kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.45.0-kafka-3.9.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server 172.17.0.7:9092 --topic my-topic
If you don't see a command prompt, try pressing enter.
>


>Hello Strimzi!
[2025-04-21 18:49:52,495] WARN [Producer clientId=console-producer] The metadata response from the cluster reported a recoverable issue with correlation id 6 : {my-topic=UNKNOWN_TOPIC_OR_PARTITION} (org.apache.kafka.clients.NetworkClient)
>Hi again
>Strimzi
>^Cpod "kafka-producer" deleted
pod kafka/kafka-producer terminated (Error)
$
$




$ kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.45.0-kafka-3.9.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server 172.17.0.7:9092 --topic my-topic --from-beginning
If you don't see a command prompt, try pressing enter.
Hello Strimzi!
Hi again
Strimzi




