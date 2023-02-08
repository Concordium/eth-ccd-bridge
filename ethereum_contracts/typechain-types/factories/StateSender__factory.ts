/* Autogenerated file. Do not edit manually. */
/* tslint:disable */
/* eslint-disable */
import { Signer, utils, Contract, ContractFactory, Overrides } from "ethers";
import { Provider, TransactionRequest } from "@ethersproject/providers";
import type { StateSender, StateSenderInterface } from "../StateSender";

const _abi = [
  {
    anonymous: false,
    inputs: [
      {
        indexed: false,
        internalType: "uint256",
        name: "id",
        type: "uint256",
      },
      {
        indexed: true,
        internalType: "address",
        name: "depositor",
        type: "address",
      },
      {
        indexed: false,
        internalType: "bytes32",
        name: "depositReceiver",
        type: "bytes32",
      },
      {
        indexed: true,
        internalType: "address",
        name: "rootToken",
        type: "address",
      },
      {
        indexed: true,
        internalType: "address",
        name: "vault",
        type: "address",
      },
      {
        indexed: false,
        internalType: "bytes",
        name: "depositData",
        type: "bytes",
      },
    ],
    name: "LockedToken",
    type: "event",
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: false,
        internalType: "uint256",
        name: "id",
        type: "uint256",
      },
      {
        indexed: false,
        internalType: "bytes32",
        name: "root",
        type: "bytes32",
      },
    ],
    name: "MerkleRoot",
    type: "event",
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: "bytes32",
        name: "role",
        type: "bytes32",
      },
      {
        indexed: true,
        internalType: "bytes32",
        name: "previousAdminRole",
        type: "bytes32",
      },
      {
        indexed: true,
        internalType: "bytes32",
        name: "newAdminRole",
        type: "bytes32",
      },
    ],
    name: "RoleAdminChanged",
    type: "event",
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: "bytes32",
        name: "role",
        type: "bytes32",
      },
      {
        indexed: true,
        internalType: "address",
        name: "account",
        type: "address",
      },
      {
        indexed: true,
        internalType: "address",
        name: "sender",
        type: "address",
      },
    ],
    name: "RoleGranted",
    type: "event",
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: "bytes32",
        name: "role",
        type: "bytes32",
      },
      {
        indexed: true,
        internalType: "address",
        name: "account",
        type: "address",
      },
      {
        indexed: true,
        internalType: "address",
        name: "sender",
        type: "address",
      },
    ],
    name: "RoleRevoked",
    type: "event",
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: false,
        internalType: "uint256",
        name: "id",
        type: "uint256",
      },
      {
        indexed: true,
        internalType: "address",
        name: "rootToken",
        type: "address",
      },
      {
        indexed: false,
        internalType: "uint64",
        name: "childTokenIndex",
        type: "uint64",
      },
      {
        indexed: false,
        internalType: "uint64",
        name: "childTokenSubIndex",
        type: "uint64",
      },
      {
        indexed: true,
        internalType: "bytes32",
        name: "tokenType",
        type: "bytes32",
      },
    ],
    name: "TokenMapAdded",
    type: "event",
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: false,
        internalType: "uint256",
        name: "id",
        type: "uint256",
      },
      {
        indexed: true,
        internalType: "address",
        name: "rootToken",
        type: "address",
      },
      {
        indexed: false,
        internalType: "uint64",
        name: "childTokenIndex",
        type: "uint64",
      },
      {
        indexed: false,
        internalType: "uint64",
        name: "childTokenSubIndex",
        type: "uint64",
      },
      {
        indexed: true,
        internalType: "bytes32",
        name: "tokenType",
        type: "bytes32",
      },
    ],
    name: "TokenMapRemoved",
    type: "event",
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: false,
        internalType: "uint256",
        name: "id",
        type: "uint256",
      },
      {
        indexed: true,
        internalType: "bytes32",
        name: "tokenType",
        type: "bytes32",
      },
      {
        indexed: true,
        internalType: "address",
        name: "vaultAddress",
        type: "address",
      },
    ],
    name: "VaultRegistered",
    type: "event",
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: false,
        internalType: "uint256",
        name: "id",
        type: "uint256",
      },
      {
        indexed: true,
        internalType: "uint64",
        name: "ccdIndex",
        type: "uint64",
      },
      {
        indexed: true,
        internalType: "uint64",
        name: "ccdSubIndex",
        type: "uint64",
      },
      {
        indexed: false,
        internalType: "uint256",
        name: "amount",
        type: "uint256",
      },
      {
        indexed: true,
        internalType: "address",
        name: "userWallet",
        type: "address",
      },
      {
        indexed: false,
        internalType: "bytes32",
        name: "ccdTxHash",
        type: "bytes32",
      },
      {
        indexed: false,
        internalType: "uint64",
        name: "ccdEventIndex",
        type: "uint64",
      },
      {
        indexed: false,
        internalType: "uint64",
        name: "tokenId",
        type: "uint64",
      },
    ],
    name: "WithdrawEvent",
    type: "event",
  },
  {
    inputs: [],
    name: "DEFAULT_ADMIN_ROLE",
    outputs: [
      {
        internalType: "bytes32",
        name: "",
        type: "bytes32",
      },
    ],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "EMITTER_ROLE",
    outputs: [
      {
        internalType: "bytes32",
        name: "",
        type: "bytes32",
      },
    ],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "address",
        name: "user",
        type: "address",
      },
      {
        internalType: "bytes32",
        name: "userCcd",
        type: "bytes32",
      },
      {
        internalType: "address",
        name: "rootToken",
        type: "address",
      },
      {
        internalType: "address",
        name: "vault",
        type: "address",
      },
      {
        internalType: "bytes",
        name: "depositData",
        type: "bytes",
      },
    ],
    name: "emitDeposit",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "bytes32",
        name: "merkleRoot",
        type: "bytes32",
      },
    ],
    name: "emitMerkleRoot",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "address",
        name: "rootToken",
        type: "address",
      },
      {
        internalType: "uint64",
        name: "childTokenIndex",
        type: "uint64",
      },
      {
        internalType: "uint64",
        name: "childTokenSubIndex",
        type: "uint64",
      },
      {
        internalType: "bytes32",
        name: "tokenType",
        type: "bytes32",
      },
    ],
    name: "emitTokenMapAdd",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "address",
        name: "rootToken",
        type: "address",
      },
      {
        internalType: "uint64",
        name: "childTokenIndex",
        type: "uint64",
      },
      {
        internalType: "uint64",
        name: "childTokenSubIndex",
        type: "uint64",
      },
      {
        internalType: "bytes32",
        name: "tokenType",
        type: "bytes32",
      },
    ],
    name: "emitTokenMapRemove",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "bytes32",
        name: "tokenType",
        type: "bytes32",
      },
      {
        internalType: "address",
        name: "vaultAddress",
        type: "address",
      },
    ],
    name: "emitVaultRegistered",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "uint64",
        name: "ccdIndex",
        type: "uint64",
      },
      {
        internalType: "uint64",
        name: "ccdSubIndex",
        type: "uint64",
      },
      {
        internalType: "uint256",
        name: "amount",
        type: "uint256",
      },
      {
        internalType: "address",
        name: "userWallet",
        type: "address",
      },
      {
        internalType: "bytes32",
        name: "ccdTxHash",
        type: "bytes32",
      },
      {
        internalType: "uint64",
        name: "ccdEventIndex",
        type: "uint64",
      },
      {
        internalType: "uint64",
        name: "tokenId",
        type: "uint64",
      },
    ],
    name: "emitWithdraw",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "bytes32",
        name: "role",
        type: "bytes32",
      },
    ],
    name: "getRoleAdmin",
    outputs: [
      {
        internalType: "bytes32",
        name: "",
        type: "bytes32",
      },
    ],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "bytes32",
        name: "role",
        type: "bytes32",
      },
      {
        internalType: "address",
        name: "account",
        type: "address",
      },
    ],
    name: "grantRole",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "bytes32",
        name: "role",
        type: "bytes32",
      },
      {
        internalType: "address",
        name: "account",
        type: "address",
      },
    ],
    name: "hasRole",
    outputs: [
      {
        internalType: "bool",
        name: "",
        type: "bool",
      },
    ],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "address",
        name: "_owner",
        type: "address",
      },
    ],
    name: "initialize",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "bytes32",
        name: "role",
        type: "bytes32",
      },
      {
        internalType: "address",
        name: "account",
        type: "address",
      },
    ],
    name: "renounceRole",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "bytes32",
        name: "role",
        type: "bytes32",
      },
      {
        internalType: "address",
        name: "account",
        type: "address",
      },
    ],
    name: "revokeRole",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "bytes4",
        name: "interfaceId",
        type: "bytes4",
      },
    ],
    name: "supportsInterface",
    outputs: [
      {
        internalType: "bool",
        name: "",
        type: "bool",
      },
    ],
    stateMutability: "view",
    type: "function",
  },
];

const _bytecode =
  "0x608060405234801561001057600080fd5b50610e90806100206000396000f3fe608060405234801561001057600080fd5b50600436106100f55760003560e01c806338745f161161009757806397131fbe1161006657806397131fbe14610203578063a217fddf14610216578063c4d66de81461021e578063d547741f1461023157600080fd5b806338745f16146101b757806338835bcb146101ca5780633fc3294a146101dd57806391d14854146101f057600080fd5b80632f2ff15d116100d35780632f2ff15d146101695780633001397d1461017e578063325f4a4a1461019157806336568abe146101a457600080fd5b806301ffc9a7146100fa57806318004b3914610122578063248a9ca314610145575b600080fd5b61010d610108366004610a41565b610244565b60405190151581526020015b60405180910390f35b610137600080516020610e3b83398151915281565b604051908152602001610119565b610137610153366004610a6b565b6000908152600160208190526040909120015490565b61017c610177366004610aa0565b61027b565b005b61017c61018c366004610aa0565b6102a7565b61017c61019f366004610ae4565b610318565b61017c6101b2366004610aa0565b6103b9565b61017c6101c5366004610b5c565b61043c565b61017c6101d8366004610a6b565b6104c8565b61017c6101eb366004610bbd565b610531565b61010d6101fe366004610aa0565b6105c1565b61017c610211366004610b5c565b6105ec565b610137600081565b61017c61022c366004610caa565b61066d565b61017c61023f366004610aa0565b610737565b60006001600160e01b03198216637965db0b60e01b148061027557506301ffc9a760e01b6001600160e01b03198316145b92915050565b60008281526001602081905260409091200154610298813361075e565b6102a283836107c2565b505050565b600080516020610e3b8339815191526102c0813361075e565b6002546102ce906001610cdb565b60028190556040519081526001600160a01b0383169084907ff31cad740255e788b7506a48c3101f392e186a6f0d1966726a2b127a273e09259060200160405180910390a3505050565b600080516020610e3b833981519152610331813361075e565b60025461033f906001610cdb565b60028190556040805191825260208201889052810185905267ffffffffffffffff848116606083015283811660808301526001600160a01b03871691818a16918b16907fc4372f08caaaaa641d4655ada1863088665c12af8220b819e3ea97cf1d15eaf79060a00160405180910390a45050505050505050565b6001600160a01b038116331461042e5760405162461bcd60e51b815260206004820152602f60248201527f416363657373436f6e74726f6c3a2063616e206f6e6c792072656e6f756e636560448201526e103937b632b9903337b91039b2b63360891b60648201526084015b60405180910390fd5b610438828261082d565b5050565b600080516020610e3b833981519152610455813361075e565b600254610463906001610cdb565b60028190556040805191825267ffffffffffffffff808716602084015285169082015282906001600160a01b038716907fded5c61f99b69fe9c5afc0334cf741a7bec305e6c665bf5f13c2222d95525abd906060015b60405180910390a35050505050565b600080516020610e3b8339815191526104e1813361075e565b6002546104ef906001610cdb565b600281905560408051918252602082018490527f48211a20c285b6b077b7917b8bb118eaa92efdfb8fa2e3b114ffa253e0b665ae910160405180910390a15050565b600080516020610e3b83398151915261054a813361075e565b600254610558906001610cdb565b600281905550826001600160a01b0316846001600160a01b0316876001600160a01b03167f09ad870164d049e9a088ad719acaeb1c8cf9091c7710f01a0fa32287bd8ff36160025489876040516105b193929190610d3e565b60405180910390a4505050505050565b60009182526001602090815260408084206001600160a01b0393909316845291905290205460ff1690565b600080516020610e3b833981519152610605813361075e565b600254610613906001610cdb565b60028190556040805191825267ffffffffffffffff808716602084015285169082015282906001600160a01b038716907f86b4452bbc76d4f9fef5a99d0720e10fb28a3d3e3d9fec679ce5060c238d4466906060016104b9565b600054610100900460ff166106885760005460ff161561068c565b303b155b6106ef5760405162461bcd60e51b815260206004820152602e60248201527f496e697469616c697a61626c653a20636f6e747261637420697320616c72656160448201526d191e481a5b9a5d1a585b1a5e995960921b6064820152608401610425565b600054610100900460ff16158015610711576000805461ffff19166101011790555b600060028190556107229083610894565b8015610438576000805461ff00191690555050565b60008281526001602081905260409091200154610754813361075e565b6102a2838361082d565b61076882826105c1565b61043857610780816001600160a01b0316601461089e565b61078b83602061089e565b60405160200161079c929190610d66565b60408051601f198184030181529082905262461bcd60e51b825261042591600401610ddb565b6107cc82826105c1565b6104385760008281526001602081815260408084206001600160a01b0386168086529252808420805460ff19169093179092559051339285917f2f8788117e7eff1d82e926ec794901d17c78024a50270940304540a733656f0d9190a45050565b61083782826105c1565b156104385760008281526001602090815260408083206001600160a01b0385168085529252808320805460ff1916905551339285917ff6391f5c32d9c69d2a47ea670b442974b53935d1edc7fd64eb21e047a839171b9190a45050565b61043882826107c2565b606060006108ad836002610dee565b6108b8906002610cdb565b67ffffffffffffffff8111156108d0576108d0610ba7565b6040519080825280601f01601f1916602001820160405280156108fa576020820181803683370190505b509050600360fc1b8160008151811061091557610915610e0d565b60200101906001600160f81b031916908160001a905350600f60fb1b8160018151811061094457610944610e0d565b60200101906001600160f81b031916908160001a9053506000610968846002610dee565b610973906001610cdb565b90505b60018111156109eb576f181899199a1a9b1b9c1cb0b131b232b360811b85600f16601081106109a7576109a7610e0d565b1a60f81b8282815181106109bd576109bd610e0d565b60200101906001600160f81b031916908160001a90535060049490941c936109e481610e23565b9050610976565b508315610a3a5760405162461bcd60e51b815260206004820181905260248201527f537472696e67733a20686578206c656e67746820696e73756666696369656e746044820152606401610425565b9392505050565b600060208284031215610a5357600080fd5b81356001600160e01b031981168114610a3a57600080fd5b600060208284031215610a7d57600080fd5b5035919050565b80356001600160a01b0381168114610a9b57600080fd5b919050565b60008060408385031215610ab357600080fd5b82359150610ac360208401610a84565b90509250929050565b803567ffffffffffffffff81168114610a9b57600080fd5b600080600080600080600060e0888a031215610aff57600080fd5b610b0888610acc565b9650610b1660208901610acc565b955060408801359450610b2b60608901610a84565b935060808801359250610b4060a08901610acc565b9150610b4e60c08901610acc565b905092959891949750929550565b60008060008060808587031215610b7257600080fd5b610b7b85610a84565b9350610b8960208601610acc565b9250610b9760408601610acc565b9396929550929360600135925050565b634e487b7160e01b600052604160045260246000fd5b600080600080600060a08688031215610bd557600080fd5b610bde86610a84565b945060208601359350610bf360408701610a84565b9250610c0160608701610a84565b9150608086013567ffffffffffffffff80821115610c1e57600080fd5b818801915088601f830112610c3257600080fd5b813581811115610c4457610c44610ba7565b604051601f8201601f19908116603f01168101908382118183101715610c6c57610c6c610ba7565b816040528281528b6020848701011115610c8557600080fd5b8260208601602083013760006020848301015280955050505050509295509295909350565b600060208284031215610cbc57600080fd5b610a3a82610a84565b634e487b7160e01b600052601160045260246000fd5b8082018082111561027557610275610cc5565b60005b83811015610d09578181015183820152602001610cf1565b50506000910152565b60008151808452610d2a816020860160208601610cee565b601f01601f19169290920160200192915050565b838152826020820152606060408201526000610d5d6060830184610d12565b95945050505050565b7f416363657373436f6e74726f6c3a206163636f756e7420000000000000000000815260008351610d9e816017850160208801610cee565b7001034b99036b4b9b9b4b733903937b6329607d1b6017918401918201528351610dcf816028840160208801610cee565b01602801949350505050565b602081526000610a3a6020830184610d12565b6000816000190483118215151615610e0857610e08610cc5565b500290565b634e487b7160e01b600052603260045260246000fd5b600081610e3257610e32610cc5565b50600019019056fe4b0ad04e4cfd06668c08542d3af105a3fa4b1a2813040bbb694bfecf2e5f5240a26469706673582212208a4405ec96be8b4459e1a65e06c080133d183cfe232d1d5688712b4b5f6c734864736f6c63430008100033";

type StateSenderConstructorParams =
  | [signer?: Signer]
  | ConstructorParameters<typeof ContractFactory>;

const isSuperArgs = (
  xs: StateSenderConstructorParams
): xs is ConstructorParameters<typeof ContractFactory> => xs.length > 1;

export class StateSender__factory extends ContractFactory {
  constructor(...args: StateSenderConstructorParams) {
    if (isSuperArgs(args)) {
      super(...args);
    } else {
      super(_abi, _bytecode, args[0]);
    }
    this.contractName = "StateSender";
  }

  deploy(
    overrides?: Overrides & { from?: string | Promise<string> }
  ): Promise<StateSender> {
    return super.deploy(overrides || {}) as Promise<StateSender>;
  }
  getDeployTransaction(
    overrides?: Overrides & { from?: string | Promise<string> }
  ): TransactionRequest {
    return super.getDeployTransaction(overrides || {});
  }
  attach(address: string): StateSender {
    return super.attach(address) as StateSender;
  }
  connect(signer: Signer): StateSender__factory {
    return super.connect(signer) as StateSender__factory;
  }
  static readonly contractName: "StateSender";
  public readonly contractName: "StateSender";
  static readonly bytecode = _bytecode;
  static readonly abi = _abi;
  static createInterface(): StateSenderInterface {
    return new utils.Interface(_abi) as StateSenderInterface;
  }
  static connect(
    address: string,
    signerOrProvider: Signer | Provider
  ): StateSender {
    return new Contract(address, _abi, signerOrProvider) as StateSender;
  }
}