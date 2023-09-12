import * as sdk from '@parsiq/datalake-sdk';
import { TsunamiApiClient, ChainId } from '@parsiq/tsunami-client';
import { TsunamiFilter, TsunamiEvent, DatalakeTsunamiApiClient } from '@parsiq/tsunami-client-sdk-http';
import { Interface } from '@ethersproject/abi';

// Import your ABIs here, in a format suitable for decoding using @ethersproject/abi.
import votingAbi from './VotingSystem.json';

// Put your Tsunami API key here.
const TSUNAMI_API_KEY = 'YOUR_API_KEY';
// This is the chain ID for Ethereum mainnet Tsunami API. Change it if you want to work with a different net.
const TSUNAMI_API_NET = ChainId.ETH_SEPOLIA;

// AAVE ETH Pool contract address, replace or drop if you intend to monitor something else.
const CONTRACT_ADDRESS = '0x7CA293451A1131D67A7dAAA0a852D5564366b7bf';

// topic_0 hashes of our events of interest.
const NEW_CANDIDATE_EVENT_TOPIC_0 = '0x01c6feb6a218293c8f849426e09abdca9d0d75e57e2255b6a7942add2bb3cb90';
const VOTE_EVENT_TOPIC_0 = '0xc00232df16a35660dbcdde113a8565e8848dc6202169f47b8f35e8c2511d40bc';

// Contract deployment block is used as a starting point for the datalake.
const CONTRACT_DEPLOYMENT_BLOCK_NUMBER = 4_274_183; // real deployment block is 10_000_835

// This defines the layout of the type-safe K-V storage.
type DatalakeStorageLayout = {
    '': any,
    candidate: { _candidateId: number, _name: string, numVotes: number },
};

type DatalakeStorageMetaLayout = {
    '': {},
    candidate: {},
};

// Types for decoded events follow.
type NewCandidateEvent = {
    _candidateId: number,
    _name: string,
}

type VoteEvent = {
    _candidateId: number,
}

class Datalake extends sdk.AbstractMultiStorageDataLakeBase<DatalakeStorageLayout, DatalakeStorageMetaLayout, TsunamiFilter, TsunamiEvent> {
    private votingDecoder: Interface;

    // Construct ABI decoders here.
    constructor() {
        super();
        this.votingDecoder = new Interface(votingAbi);
    }

    public override getProperties(): sdk.DataLakeProperties {
        return {
            id: 'DATALAKE-TEMPLATE',
            initialBlockNumber: CONTRACT_DEPLOYMENT_BLOCK_NUMBER,
        };
    }

    // This method generates the filter used to retrieve events from Tsunami. Filter may change from block to block.
    public async genTsunamiFilterForBlock(block: sdk.Block & sdk.DataLakeRunnerState, isNewBlock: boolean): Promise<TsunamiFilter> {
        return {
            contract: [CONTRACT_ADDRESS],
            topic_0: [NEW_CANDIDATE_EVENT_TOPIC_0, VOTE_EVENT_TOPIC_0],
        };
    }

    // Main event handler.
    public async processTsunamiEvent(event: TsunamiEvent & sdk.TimecodedEvent & sdk.DataLakeRunnerState): Promise<void | TsunamiFilter> {
        switch (event.topic_0) {
            case NEW_CANDIDATE_EVENT_TOPIC_0:
                await this.processNewCandidateEvent(event);
                break;
            case VOTE_EVENT_TOPIC_0:
                await this.processVoteEvent(event);
                break;
        }
    }

    private async processNewCandidateEvent(event: TsunamiEvent): Promise<void> {
        // Decodes the event...
        const fragment = this.votingDecoder.getEvent(event.topic_0!);
        const decoded = this.votingDecoder.decodeEventLog(fragment, event.log_data!, [
            event.topic_0!,
            event.topic_1!
        ]) as unknown as NewCandidateEvent;
        // ...then writes to reogranization-aware K-V storage.
        await this.set('candidate', decoded._candidateId.toString(), { _candidateId: decoded._candidateId, _name: decoded._name, numVotes: 0 });
    }

    private async processVoteEvent(event: TsunamiEvent): Promise<void> {
        // Decodes the event...
        const fragment = this.votingDecoder.getEvent(event.topic_0!);
        const decoded = this.votingDecoder.decodeEventLog(fragment, event.log_data!, [
            event.topic_0!,
            event.topic_1!
        ]) as unknown as VoteEvent;
        // ...then writes to reogranization-aware K-V storage.
        const candidate = await this.get('candidate', decoded._candidateId.toString());
        if (candidate) {
            candidate.numVotes += 1;
            await this.set('candidate', decoded._candidateId.toString(), candidate);
        }
    }

    // The following event handlers should be no-ops under most circumstances.
    public async processEndOfBlockEvent(event: sdk.Block & sdk.DataLakeRunnerState): Promise<void> {}
    public async processBeforeDropBlockEvent(event: sdk.DropBlockData & sdk.DataLakeRunnerState): Promise<void> {}
    public async processAfterDropBlockEvent(event: sdk.DropBlockData & sdk.DataLakeRunnerState): Promise<void> {}
}

export const run = async (): Promise<void> => {
    const logger = new sdk.ConsoleLogger();
    logger.log('DEBUG', 'Initializing datalake...');
    const datalake = new Datalake();
    logger.log('DEBUG', 'Initializing Tsunami API...');
    const tsunami = new TsunamiApiClient(TSUNAMI_API_KEY, TSUNAMI_API_NET);
    logger.log('DEBUG', 'Initializing SDK Tsunami client...');
    const tsunamiSdk = new DatalakeTsunamiApiClient(tsunami);
    logger.log('DEBUG', 'Initializing runner...');
    const runner = new sdk.MultiStorageDataLakeRunner({
        storageConfig: {
            '': { meta: {} },
            'candidate': { meta: {} },
        },
        datalake: datalake,
        tsunami: tsunamiSdk,
        log: logger,
    });
    logger.log('DEBUG', 'Running...');
    await runner.run();
}
