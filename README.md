# GitOps Core

## This is complicated, where should I go?

Depending on what you're trying to do, you'll need to go to the appropriate directory to affect changes.

* bases
  - Core structure of Kubernetes Kustomizations, no environment specific configuration, changes here apply to ALL clusters, production included.
* overlays
  - Environment specific configuration of Kustomizations from the `bases` directory.
* clusters
  - Informs Flux what it should be deploying, each instance of Flux monitors a different subdirectory here.

## Bootstrapping and Upgrading

### Bootstrapping

Flux gets installed by Chef during the Cluster creation phase with a set of pre-defined YAML files, once Flux syncs with this repository it'll upgrade itself to whatever is defined here and deploy all relevant projects.

### Upgrading

Flux can be upgraded in Dev with the following command

```shell
$ export GITHUB_TOKEN=<token>
$ flux bootstrap github \
          --components-extra=image-reflector-controller,image-automation-controller \
          --owner=binkhq \
          --repository=gitops \
          --branch=master \
          --path=clusters/uksouth-dev
```

We then use the following `cp` command to copy the manifest changes to all other environments (fish syntax):

```shell
$ for i in clusters/*
       cp clusters/uksouth-dev/flux-system/gotk-components.yaml $i/flux-system/gotk-components.yaml
  end
```