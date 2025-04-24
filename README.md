# slack-export-file-downloader
For downloading file attachments listed in slack workspace export json files.

Extracts `URL_private_download` values from JSON files.
Downloads unique files, appends the Slack-provided ID to the end of the filename for files with non-unique names.
Skips files that already exist in the target download location.

Example usage:

```bash
Slack_export_parser.py "C:\temp\channelName\" --parse --download --download_folder 'downloads'
```
