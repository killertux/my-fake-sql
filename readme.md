### My Fake SQL

Creates a Fake SQL databases and uses Runops to execute the queries.

## Set-up

First, start by installing and login into the runops cli. More details [here](https://runops.io/docs/developers/#setup).

Than, copy `config.yml.example` as `config.yml` and edit is as you want.

Than, you will need to install rust and cargo in your machine. Details [here](https://rustup.rs/).

Finally, you can just type `cargo run` and the fake mysql server will start. You can also compile it as release and run it using `cargo run --release`.

## Connecting with JetBrains

Create a new MySQL data source. As the host use `127.0.0.1` and use the port that you configured in the `config.yml`file. Add a stub user and password. DO NOT type a database.

This should be enough.

## Throubleshooting

Follow [this](https://stackoverflow.com/questions/52522565/git-is-not-working-after-macos-update-xcrun-error-invalid-active-developer-pa?answertab=scoredesc#tab-top) answer if you got:  
> note: xcrun: error: invalid active developer path (/Library/Developer/CommandLineTools), missing xcrun at: /Library/Developer/CommandLineTools/usr/bin/xcrun