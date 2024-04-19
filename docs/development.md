# Development

## Installation for development

To develop Astra, it is recommended to install it locally as a Python package with its development and testing dependencies.

```bash
pip install -e "{path_to_astra_clone}[dev, test]"
```

## Building the documentation

To build the documentation, you need to install the documentation dependencies.

```bash
pip install -e "{path_to_astra_clone}[docs]"
```

Then, you can build the documentation with

```bash
cd {path_to_astra_clone}/docs
make clean html
```