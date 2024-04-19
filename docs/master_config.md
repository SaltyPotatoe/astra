# Configuration

The astra software is configure through the yaml configuration file `~/.astra/config.yaml` with the following fields:
```yaml
# path of the folder where astra reads and writes data
folder_assets: /Users/lgrcia/code/dev/astra/assets

# path of the local gaia database used by astra for the pointing model
gaia_db: null
```

Under the assets folder, the following strucure is created:
```
assets
├── images        # where astra saves the images
├── log           # log-related files
├── observatory   # observatory config files
└── schedule      # schedule files
```