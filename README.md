# GitOps Core

## Bootstrapping

We're currently not bootstrapping Flux 2 within clusters with Chef at cluster creation, as such, see example command below to bootstrap a new cluster. Available environments are pretty much the output of `ls clusters`.

```shell
$ export GITOPS_ENV=uksouth-dev
$ flux bootstrap gitlab \
    --components-extra=image-reflector-controller,image-automation-controller \
    --hostname=git.bink.com \
    --owner=GitOps \
    --repository=core \
    --branch=master \
    --path=clusters/$GITOPS_ENV
```
