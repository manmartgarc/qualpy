import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="qualpy",
    version="0.0.1",
    install_requires=[
        'pandas',
        'requests'
        ],
    author="Manuel Martinez",
    author_email="manmartgarc@gmail.com",
    description="Thin python wrapper for the Qualtrics API platform.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/manmartgarc/qualpy",
    project_urls={
        "Bug Tracker": "https://github.com/manmartgarc/qualpy/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(),
    python_requires=">=3.9",
    keywords=[
        'qualtrics',
        'qualpy',
    ]
)
