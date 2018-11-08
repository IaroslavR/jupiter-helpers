from setuptools import setup

with open("README.md") as f:
    long_description = f.read()

setup(
    name="sagemaker-helpers",
    version="0.1",
    description="helpers for sagemaker notebooks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
    ],
    url="https://github.com/IaroslavR/sagemaker-helpers",
    author="Iaroslav Russkikh",
    author_email="iarruss@ya.ru",
    license="MIT",
    packages=["sagemaker_helpers"],
    install_requires=["attrs", "structlog", "boto3==1.7.82", "sagemaker", "requests"],
    include_package_data=True,
    zip_safe=False,
)
