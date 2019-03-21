# Configuration

Krews requires configuration files to run. These files, along with command line arguments allow you to tweak *how*
your workflow runs without touching or building your Krews application. The application itself is more concerned 
with *what* runs. In this way it is a means of 
[separation of concerns](https://en.wikipedia.org/wiki/Separation_of_concerns). 

Configurations cover things like execution environment settings and parallelism. They also cover special parameters 
that you set up in your Krews Application to pass in differently for each run. They allow you to provide your 
workflow to 3rd parties as an executable. The only thing your users would then need to worry about is creating 
a config.

Krews configuration files are written in a superset of JSON called 
[HOCON](https://github.com/lightbend/config/blob/master/HOCON.md). It's just JSON with some additional syntax sugar 
that helps keep it concise and readable. These files conventionally use the extension `.conf`.

There are two types of configurations, workflow scope and task scope.

## Workflow Configurations

Workflow scope configurations apply to the entire workflow. They live at the top level of your configuration documents.

### Common Workflow Configurations

config name | description | required | default
--- | --- | --- | ---
parallelism | The maximum number of tasks allowed to run concurrently. Set to an integer or "unlimited" | no | unlimited 
db-upload-delay | Delay (in seconds) between uploading the latest database to project working storage | no | 60
report-generation-delay | Delay (in seconds) between generating updated status reports | no | 60

**Example**

```hocon
parallelism = 10
db-upload-delay = 120
report-generation-delay = 180
```

### Local Docker Execution Specific

name | description | required | default
--- | --- | --- | ---
local.workingDir | Working directory for local docker run | no | working-dir
local.docker | Docker client configurations. In most cases you should not need to set these. | no | *none*
local.docker.uri | URI of docker daemon | no | *none*
local.docker.certificates-path | Path to certificate for TLS enabled docker daemon | no | *none*
local.docker.connect-timeout | Timeout (in milliseconds) for connecting to docker daemon | no | 5000
local.docker.read-timeout | Timeout (in milliseconds) for reading data from docker daemon | no | 30000
local.docker.connection-pool-size | Connection pool size for docker client | no | 100

**Example**

```hocon
local {
    working-dir = some-test/directory
    docker {
        uri = "localhost:1234"
        certificate-path = "~/my-path/my-cert.cert"
        connect-timeout = 10000
        read-timeout = 60000
        connection-pool-size = 200
    }
}
```

### Google Cloud Execution Specific

name | description | required | default
--- | --- | --- | ---
google.project-id | Google Cloud project ID | yes | *none*
google.regions | List of regions in which we're allow to create VMs | no | *none*
google.storage-bucket | Google Cloud Storage bucket where we will be keeping our workflow files | yes | *none*
google.storage-base-dir | Google Cloud Storage base directory where we will be keeping our workflow files (in the given bucket) | yes | *none*
google.job-completion-poll-interval | Interval (in seconds) between checks for pipeline job completion | no | 10
google.log-upload-interval | Interval (in seconds) between  pipeline job log uploads to storage | no | 60

**Example**

```hocon
google {
    project-id = my-project
    regions = [us-east1, us-east2]
    storage-bucket = my-bucket
    storage-base-dir = my-base/directory
    job-completion-poll-interval = 60
    log-upload-interval = 120
}
```

### Slurm Execution Specific

name | description | required | default
--- | --- | --- | ---
slurm.working-dir | Working directory for local docker run. Must be an NFS mounted directory accessible from workers | yes | *none*
slurm.job-completion-poll-interval | Interval (in seconds) between checks for pipeline job completion | no | 10
slurm.ssh | Used to connect to slurm head node to run sbatch jobs and poll job status. Enables running remote machines that can ssh to slurm head node with passwordless login | no | *none*
slurm.ssh.user | User for slurm head node ssh | no | *none*
slurm.ssh.host | Slurm head node host name | no | *none*
slurm.ssh.port | Slurm head node ssh port | no | 22

**Example**

```hocon
slurm {
    working-dir = /data/my-dir
    job-completion-poll-interval = 30
    ssh {
        user = jbrooks
        host = slurm018
        port = 23
    }
}
```

## Task Configurations
