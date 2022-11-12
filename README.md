# playground-apachebeam

## Dataflow

Dataflow へパイプラインを構築する方法は２つある。１つ目は CLI 経由で直接 Dataflow へパイプラインを構築する方法。２つ目は テンプレート経由で Job をインポートし、値を指定する方法。

### via CLI

https://cloud.google.com/pubsub/docs/stream-messages-dataflow#python_1

### via Classic Template

Classic Template の方法ではテンプレート自体を、ValueProvider での受け渡し方法へ変更し、値を Job 作成時に指定可能にしている。が、Pub/Sub のトピック及びサブスクリプションはこの Value Provider での受け渡し方法をサポートしていないので、Flex Template を使うしか無い。

> A classic template contains the JSON serialization of a Dataflow job graph. The code for the pipeline must wrap any runtime parameters in the ValueProvider interface. This interface allows users to specify parameter values when they deploy the template. To use the API to work with classic templates, see the projects.locations.templates API reference documentation.

https://cloud.google.com/dataflow/docs/guides/templates/creating-templates

### via Flex Template

Flex Template は Template Build、Template Run という2つのステップで Dataflow Job を作成する。Build 時には、Docker Image を Build する都合上。必ずローカルのコマンドを叩かなくてはならない。また、この Build は Terraform 化できない。よって、シンプルに Classic Template で CLI から Job を作成するのが一番ラクかもしれない。

https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates

逆に言えば、予めどこかの環境に Build 済みの Flex Template を用意しておけば Terraform 化できる。