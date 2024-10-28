# NASA Earth Exchange Global Daily Downscaled Projections (NEX-GDDP-CMIP6)

For a detailed description of this dataset, see:
https://www.nccs.nasa.gov/services/data-collections/land-based-products/nex-gddp-cmip6

[nex-gddp-cmip6.txt](nex-gddp-cmip6.txt) is a listing of the data set's
bucket hosted by Amazon's Open Data registry. The listing can be generated
by the [AWS CLI](https://docs.aws.amazon.com/cli/latest/reference/s3/):

    aws s3 ls --recursive --no-sign-request s3://nex-gddp-cmip6 > nex-gddp-cmip6.txt