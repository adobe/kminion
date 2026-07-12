# Helm Chart

⚠️ This chart has been moved to https://github.com/redpanda-data/helm-charts/tree/main/charts/kminion . Please install this chart instead. The existing archives are still being hosted here, to not break existing deployments.

---

This chart is intentionally very light on input validation. The goal was to offer a flexible Helm chart that allows
users to deploy KMinion the way they want to. Therefore it's very flexible at the cost of less input validation, so that
you might run into runtime errors for a misconfiguration.

All available input is documented inside of the [values.yaml](./kminion/values.yaml) file.

## Secrets

`kminion.config` is rendered verbatim into a Kubernetes ConfigMap, which is stored unencrypted and readable by
any principal with `configmap get/list` RBAC in the release namespace. Do not put credentials there — this
includes `sasl.password`, `sasl.gssapi.password`, `tls.passphrase`, `tls.key`, and OAuth client secrets.

Instead, use `deployment.env.secretKeyRefs` to inject credentials from a Kubernetes Secret as environment
variables. See the commented example under `deployment.env` in [values.yaml](./kminion/values.yaml), and the
[reference config](https://github.com/cloudhut/kminion/blob/master/docs/reference-config.yaml) for the
expected environment variable names.

## Installing the Helm chart

```shell
helm repo add kminion https://raw.githubusercontent.com/cloudhut/kminion/master/charts/archives
helm repo update
helm install -f values.yaml kminion kminion/kminion
```
