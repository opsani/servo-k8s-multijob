# servo-k8s-multijob
PoC servo with integrated measure for monitoring execution time of k8s one-time jobs (restartPolicy=Never)

This project implements a cut-down 'servo' daemon (similar to git:git@github.com:opsani/servo) with integrated 'measure' function that collects run time of pods that match the following criteria:
- run in a specific namespace (configured when starting the daemon)
- have an env variable named "JOBID" set to a non-empty value

The daemon is primarily intended to be run in a container. For convenient access to the Kubernetes API, it should be started as a pod or a deployment, with a configured service account that gives it automatic access to the API. See the `example` directory, it contains a set of files that can be used with 'kubectl create' to set up the necessary service account, the RBAC trust objects and a deployment running the servo container.

## Building the container

From the root directory of this repo, run (change the -t value as desired, it should be a tag that you can push to your Docker registry):

    tag=$docker-acct/servo-k8s-multijob:latest
    docker build -t "$tag" .
    docker push "$tag"

## Configuration

The following environment variables can be set to configure the daemon (if running in a deployment, you can add them to 'env' in the container spec):

OPTUNE\_URL url to post data to. The string can contain {acct} and {app\_id} replacement strings, these will be set to the value of $OPTUNE\_ACCOUNT and the pod's JOBID env variable, respectively. This defaults to "https://us-central1-optune-saas-collect.cloudfunctions.net/metrics/{acct}/{app\_id}/servo". NOTE the default URL requires an authentication token, contact the Opsani to get one.

OPTUNE\_ACCOUNT a string to be inserted in the target URL; this should contain only valid URL path characters, without /.

OPTUNE\_AUTH\_TOKEN a 'Bearer' token to send with the post request. If this is not set, no 'Authorization' header will be sent.

POD\_NAMESPACES a comma-separated list of namespaces to watch. If not set, "default" is assumed.

OPTUNE\_APP\_ID the location of a string value in the pod object that is recognized as the 'application ID'. This is specified as a filepath-like string. Each path element is a map key or array index to use in a sequence to descend to the required value. A path element in the form [key] is used to search an array containing maps in the form {"name":k, "value":v} (as used in the container spec's 'env' object). The default is: `"spec/containers/0/env/[JOBID]"`.

## Example configuration for running in a k8s deployment

The `example/` directory contains files that can be used to configure and run the daemon as a deployment on your k8s cluster.

The svc-acct.yaml and svc-role.yaml files set up a trusted service account giving access to designated deployments to access the k8s API; these can be used as they are or edited if needed; the deployment.yaml file must be copied and edited according to your setup.

    # === trust setup
    # this creates a service account named opsani-servo:
    kubectl create -f svc-acct.yaml
    # this creates a role 'opsani-servo-role' with access to pod objects
    # in all namespaces and then gives the opsani-servo account that role:
    kubectl create -f svc-role.yaml

Copy and modify deployment.yaml and set the following in spec/template/spec/containers/:
`image:` the image tag, as built and pushed above
`env/["name":"OPTUNE_AUTH_TOKEN"]/value:` your API access token
`env/["name":"OPTUNE_ACCOUNT"]/value:` a fully-qualified DNS name that identifies you or your application (or another unique string that can be used in a URL path). NOTE, in the future Opsani may require this to be a string assigned to you explicitly.

Other env variable settings can be added, if desired, see **Configuration** above.

Create the deployment:

    kubectl create -f my-deployment.yaml

## Enabling Initializer Configuration

(EXPERIMENTAL, NOT NEEDED to run the current version of this servo)

The servo daemon recognizes initializer configuration named `initcfg.optune.io`, if one is present in metadata/initializers/pending in newly-created pods.

Currently, it does nothing other than remove the initializer annotation to let the pod run. The support is indended to test the initializer setup and for future support in the servo to apply configuration settings to pods that are about to start, based on an 'ADJUST' command received from the Optune service.

Below is an example initializer configuration object that can be created with 'kubectl -f create'. NOTE this should be created only *after* starting the daemon, to ensure it does not block the startup of new pods. If the daemon is not running, new pods that are created will remain uninitialized and will not run.

    apiVersion: admissionregistration.k8s.io/v1alpha1
    kind: InitializerConfiguration
    metadata:
      name: optune
    initializers:
      # the name needs to be fully qualified, i.e., containing at least two "."
      - name: initcfg.optune.io
        rules:
          # apiGroups, apiVersion, resources all support wildcard "*".
          # "*" cannot be mixed with non-wildcard.
          - apiGroups:
    	  - ""
    	apiVersions:
    	  - "*"
    	resources:
    	  - pods


