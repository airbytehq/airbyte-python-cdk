# Configuring the Debugger

## For VSCode

### Set Up the debugger configuration (one-time setup step)

To configure the debugger in VSCode to run the `debug_manifest`, follow these steps:

1. Clone or Open the existing `airbyte-python-cdk` project in VSCode.
2. Click on the Run and Debug icon in the Activity Bar on the side of the window.
3. Click on the `create a launch.json file` link to create a new configuration file.
4. Select `Python` from the list of environments.
5. Replace the contents of the generated `launch.json` file with the following configuration:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Python: Debug Manifest",
      "type": "debugpy",
      "request": "launch",
      "console": "integratedTerminal",
      "cwd": "${workspaceFolder}/debug_manifest",
      "python": "<PATH_TO_CDK_ENV>/bin/python", // REPLACE ME
      "module": "debug_manifest",
      "args": [
        // SPECIFY THE COMMAND: [spec, check, discover, read]
        "read",
        // SPECIFY THE MANIFEST FILE
        "--manifest-path",
        // PATH TO THE MANIFEST FILE
        "resources/manifest.yaml",
        // SPECIFY THE CONFIG
        "--config",
        // PATH TO THE CONFIG FILE
        "resources/config.json",
        // SPECIFY THE CATALOG
        "--catalog",
        // PATH TO THE CATALOG FILE
        "resources/catalog.json",
        // SPECIFY THE STATE (optional)
        // "--state",
        // PATH TO THE STATE FILE
        // "resources/state.json",
        // ADDITIONAL FLAGS, like `--debug` (optional)
        "--debug"
      ]
    }
  ]
}
```

6. Save the `launch.json` file.
7. Install `CDK dependencies` by running `poetry install --all-extras`
8. Replace the `"python": "<PATH_TO_CDK_ENV>/bin/python"` with the correct interpreter `PATH` pointing to the `CDK env` installed from Step `7` (use `which python` to have the complete python path), to wire the CDK env to the debugger. Alternatively you can switch the default interpreter you use in your IDE.

### Set up the necessary resources to use within the manifest-only connector

- These resources are ignored by `git`, in the `.gitignore`, thus should not be committed

1. Put the `config.json` inside the `/airbyte_cdk/debug_manifest/resources` (this will hold the `source input configuration`).
2. Put the `catalog.json` inside the `/airbyte_cdk/debug_manifest/resources` (this will hold the `configured catalog` for the target source).
3. Put the `manifest.yaml` inside the `/airbyte_cdk/debug_manifest/resources`
4. (Optional) Put the `state.json` inside the `/airbyte_cdk/debug_manifest/resources`

## Debugging Steps

1. Set any necessary breakpoints in your code, or `CDK` components code.
2. Press `F5` / `Shift + CMD + D` / click the green play button in the `Run and Debug` view to start debugging.
3. Iterate over the `2` and `3`, to debug your `manifest-only` source.

Basically, you're now able to run the `manifest-only` sources like the regular python source, with `spec`, `check`, `discover` and `read` commands, as well as having the `--debug` alongside.
