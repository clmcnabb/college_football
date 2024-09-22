import setuptools

setuptools.setup(
    name="cfdb-merging-pipeline",
    version="0.0.1",
    description="Pipeline package for merging cfdb files.",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        "google-apitools",
        "google-cloud-core",
        "google-cloud-storage",
        "apache-beam",
    ],
)
