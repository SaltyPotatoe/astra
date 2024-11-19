# Installation

Clone the Astra Github repository with

```bash
git clone https://github.com/ppp-one/astra.git
```

then, depending on your use case, install Astra with the following methods.

## pip
It is highly recommended to install Astra in a virtual/conda environment. Start by creating such an environment and activate it with

```bash
conda create -n astra python=3.11
conda activate astra
```

then install Astra locally with

```bash
pip install -e {path_to_astra_clone}
```

## Testing installation 
> only if installed as a local python package with pip

Once installed, activate your environment (highly recommended) and run

```bash
conda activate astra
python -c "import astra"
```

If no error is raised you're all set!