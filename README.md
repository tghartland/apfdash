# APF Dashboard

### Run

`python index.py`

Requires Python 3.

### Environment Variables

| Variable | Description |
| --- | --- |
| `APFDASH_AWS_KEY` | AWS access key |
| `APFDASH_AWS_SECRET` | AWS access secret |
| `APFDASH_AWS_REGION` | AWS region e.g `"us-east-1"` |
| `APFDASH_URL_PREFIX` | URL prefix to use, for instance `"/dash/"` if the URL path <br> /dash/ of another sever is being redirected to the Dash application |

The AWS variables can be ignored if there is an AWS credentials file in the standard
location `~/.aws/credentials`. If the key variable *is* set, it will be assumed that
the other two are set as well.

`APFDASH_URL_PREFIX` defaults to `"/"` if it is not set.