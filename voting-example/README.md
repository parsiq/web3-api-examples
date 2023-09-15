# Quick start

Before you can run anything, you must change the `TSUNAMI_API_KEY` constant in `src/datalake.ts` to your private Tsunami key.

`src/datalake.ts` implements a very simple stateful datalake monitoring the Voting contract deployed on Ethereum Sepolia testnet at this address: 0x7CA293451A1131D67A7dAAA0a852D5564366b7bf. The contract allows to add candidates and vote for them. The datalake tracks the number of votes for each candidate. The code is annotated to help you modify it for your purposes.

To install packages (this step is not necessary, but helps a lot because without it autocompletion does not work):

```
npm install
```

After running `npm install` you can find the README file for the datalake SDK at `node_modules/@parsiq/datalake-sdk/README.md`.

To install Docker:

```
sudo apt install docker docker-compose
```

To build the runner image:

```
sudo docker build -t datalake-template-dev .
```

To run:

```
sudo docker-compose up
```

The datalake is currently configured to reset its state on each run. To change that behavior, as well as other configuration details, take a look at `docker-compose.yml`.

You can inspect the current state of the datalake using:

```
psql postgresql://datalake:datalake@localhost:54329/datalake
```

The `candidate_entities` table contains mappings of token IDs to candidate data, which is a name and a number of votes. The tables with names ending in `_mutations` contain historical data.

Have fun!
