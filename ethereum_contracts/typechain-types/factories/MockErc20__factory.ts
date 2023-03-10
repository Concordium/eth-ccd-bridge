/* Autogenerated file. Do not edit manually. */
/* tslint:disable */
/* eslint-disable */
import { Signer, utils, Contract, ContractFactory, Overrides } from "ethers";
import { Provider, TransactionRequest } from "@ethersproject/providers";
import type { MockErc20, MockErc20Interface } from "../MockErc20";

const _abi = [
  {
    inputs: [],
    stateMutability: "nonpayable",
    type: "constructor",
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: "address",
        name: "owner",
        type: "address",
      },
      {
        indexed: true,
        internalType: "address",
        name: "spender",
        type: "address",
      },
      {
        indexed: false,
        internalType: "uint256",
        name: "value",
        type: "uint256",
      },
    ],
    name: "Approval",
    type: "event",
  },
  {
    anonymous: false,
    inputs: [
      {
        indexed: true,
        internalType: "address",
        name: "from",
        type: "address",
      },
      {
        indexed: true,
        internalType: "address",
        name: "to",
        type: "address",
      },
      {
        indexed: false,
        internalType: "uint256",
        name: "value",
        type: "uint256",
      },
    ],
    name: "Transfer",
    type: "event",
  },
  {
    inputs: [
      {
        internalType: "address",
        name: "owner",
        type: "address",
      },
      {
        internalType: "address",
        name: "spender",
        type: "address",
      },
    ],
    name: "allowance",
    outputs: [
      {
        internalType: "uint256",
        name: "",
        type: "uint256",
      },
    ],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "address",
        name: "spender",
        type: "address",
      },
      {
        internalType: "uint256",
        name: "amount",
        type: "uint256",
      },
    ],
    name: "approve",
    outputs: [
      {
        internalType: "bool",
        name: "",
        type: "bool",
      },
    ],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "address",
        name: "account",
        type: "address",
      },
    ],
    name: "balanceOf",
    outputs: [
      {
        internalType: "uint256",
        name: "",
        type: "uint256",
      },
    ],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "decimals",
    outputs: [
      {
        internalType: "uint8",
        name: "",
        type: "uint8",
      },
    ],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "address",
        name: "spender",
        type: "address",
      },
      {
        internalType: "uint256",
        name: "subtractedValue",
        type: "uint256",
      },
    ],
    name: "decreaseAllowance",
    outputs: [
      {
        internalType: "bool",
        name: "",
        type: "bool",
      },
    ],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "address",
        name: "spender",
        type: "address",
      },
      {
        internalType: "uint256",
        name: "addedValue",
        type: "uint256",
      },
    ],
    name: "increaseAllowance",
    outputs: [
      {
        internalType: "bool",
        name: "",
        type: "bool",
      },
    ],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "address",
        name: "to",
        type: "address",
      },
      {
        internalType: "uint256",
        name: "amount",
        type: "uint256",
      },
    ],
    name: "mint",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [],
    name: "name",
    outputs: [
      {
        internalType: "string",
        name: "",
        type: "string",
      },
    ],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "uint8",
        name: "decimalsValue",
        type: "uint8",
      },
    ],
    name: "setDecimals",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [],
    name: "symbol",
    outputs: [
      {
        internalType: "string",
        name: "",
        type: "string",
      },
    ],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "totalSupply",
    outputs: [
      {
        internalType: "uint256",
        name: "",
        type: "uint256",
      },
    ],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "address",
        name: "to",
        type: "address",
      },
      {
        internalType: "uint256",
        name: "amount",
        type: "uint256",
      },
    ],
    name: "transfer",
    outputs: [
      {
        internalType: "bool",
        name: "",
        type: "bool",
      },
    ],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [
      {
        internalType: "address",
        name: "from",
        type: "address",
      },
      {
        internalType: "address",
        name: "to",
        type: "address",
      },
      {
        internalType: "uint256",
        name: "amount",
        type: "uint256",
      },
    ],
    name: "transferFrom",
    outputs: [
      {
        internalType: "bool",
        name: "",
        type: "bool",
      },
    ],
    stateMutability: "nonpayable",
    type: "function",
  },
];

const _bytecode =
  "0x60806040523480156200001157600080fd5b50604051806040016040528060048152602001634d4f434b60e01b815250604051806040016040528060028152602001614d4f60f01b81525081600390816200005b919062000125565b5060046200006a828262000125565b50506005805460ff1916601217905550620001f1565b634e487b7160e01b600052604160045260246000fd5b600181811c90821680620000ab57607f821691505b602082108103620000cc57634e487b7160e01b600052602260045260246000fd5b50919050565b601f8211156200012057600081815260208120601f850160051c81016020861015620000fb5750805b601f850160051c820191505b818110156200011c5782815560010162000107565b5050505b505050565b81516001600160401b0381111562000141576200014162000080565b620001598162000152845462000096565b84620000d2565b602080601f831160018114620001915760008415620001785750858301515b600019600386901b1c1916600185901b1785556200011c565b600085815260208120601f198616915b82811015620001c257888601518255948401946001909101908401620001a1565b5085821015620001e15787850151600019600388901b60f8161c191681555b5050505050600190811b01905550565b610a3880620002016000396000f3fe608060405234801561001057600080fd5b50600436106100cf5760003560e01c806340c10f191161008c57806395d89b411161006657806395d89b41146101c4578063a457c2d7146101cc578063a9059cbb146101df578063dd62ed3e146101f257600080fd5b806340c10f191461016257806370a08231146101775780637a1395aa146101a057600080fd5b806306fdde03146100d4578063095ea7b3146100f257806318160ddd1461011557806323b872dd14610127578063313ce5671461013a578063395093511461014f575b600080fd5b6100dc61022b565b6040516100e9919061085f565b60405180910390f35b6101056101003660046108c9565b6102bd565b60405190151581526020016100e9565b6002545b6040519081526020016100e9565b6101056101353660046108f3565b6102d7565b60055460405160ff90911681526020016100e9565b61010561015d3660046108c9565b6102fb565b6101756101703660046108c9565b61033a565b005b61011961018536600461092f565b6001600160a01b031660009081526020819052604090205490565b6101756101ae366004610951565b6005805460ff191660ff92909216919091179055565b6100dc610348565b6101056101da3660046108c9565b610357565b6101056101ed3660046108c9565b6103ee565b610119610200366004610974565b6001600160a01b03918216600090815260016020908152604080832093909416825291909152205490565b60606003805461023a906109a7565b80601f0160208091040260200160405190810160405280929190818152602001828054610266906109a7565b80156102b35780601f10610288576101008083540402835291602001916102b3565b820191906000526020600020905b81548152906001019060200180831161029657829003601f168201915b5050505050905090565b6000336102cb8185856103fc565b60019150505b92915050565b6000336102e5858285610520565b6102f08585856105b2565b506001949350505050565b3360008181526001602090815260408083206001600160a01b03871684529091528120549091906102cb90829086906103359087906109e1565b6103fc565b6103448282610780565b5050565b60606004805461023a906109a7565b3360008181526001602090815260408083206001600160a01b0387168452909152812054909190838110156103e15760405162461bcd60e51b815260206004820152602560248201527f45524332303a2064656372656173656420616c6c6f77616e63652062656c6f77604482015264207a65726f60d81b60648201526084015b60405180910390fd5b6102f082868684036103fc565b6000336102cb8185856105b2565b6001600160a01b03831661045e5760405162461bcd60e51b8152602060048201526024808201527f45524332303a20617070726f76652066726f6d20746865207a65726f206164646044820152637265737360e01b60648201526084016103d8565b6001600160a01b0382166104bf5760405162461bcd60e51b815260206004820152602260248201527f45524332303a20617070726f766520746f20746865207a65726f206164647265604482015261737360f01b60648201526084016103d8565b6001600160a01b0383811660008181526001602090815260408083209487168084529482529182902085905590518481527f8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925910160405180910390a3505050565b6001600160a01b0383811660009081526001602090815260408083209386168352929052205460001981146105ac578181101561059f5760405162461bcd60e51b815260206004820152601d60248201527f45524332303a20696e73756666696369656e7420616c6c6f77616e636500000060448201526064016103d8565b6105ac84848484036103fc565b50505050565b6001600160a01b0383166106165760405162461bcd60e51b815260206004820152602560248201527f45524332303a207472616e736665722066726f6d20746865207a65726f206164604482015264647265737360d81b60648201526084016103d8565b6001600160a01b0382166106785760405162461bcd60e51b815260206004820152602360248201527f45524332303a207472616e7366657220746f20746865207a65726f206164647260448201526265737360e81b60648201526084016103d8565b6001600160a01b038316600090815260208190526040902054818110156106f05760405162461bcd60e51b815260206004820152602660248201527f45524332303a207472616e7366657220616d6f756e7420657863656564732062604482015265616c616e636560d01b60648201526084016103d8565b6001600160a01b038085166000908152602081905260408082208585039055918516815290812080548492906107279084906109e1565b92505081905550826001600160a01b0316846001600160a01b03167fddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef8460405161077391815260200190565b60405180910390a36105ac565b6001600160a01b0382166107d65760405162461bcd60e51b815260206004820152601f60248201527f45524332303a206d696e7420746f20746865207a65726f20616464726573730060448201526064016103d8565b80600260008282546107e891906109e1565b90915550506001600160a01b038216600090815260208190526040812080548392906108159084906109e1565b90915550506040518181526001600160a01b038316906000907fddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef9060200160405180910390a35050565b600060208083528351808285015260005b8181101561088c57858101830151858201604001528201610870565b506000604082860101526040601f19601f8301168501019250505092915050565b80356001600160a01b03811681146108c457600080fd5b919050565b600080604083850312156108dc57600080fd5b6108e5836108ad565b946020939093013593505050565b60008060006060848603121561090857600080fd5b610911846108ad565b925061091f602085016108ad565b9150604084013590509250925092565b60006020828403121561094157600080fd5b61094a826108ad565b9392505050565b60006020828403121561096357600080fd5b813560ff8116811461094a57600080fd5b6000806040838503121561098757600080fd5b610990836108ad565b915061099e602084016108ad565b90509250929050565b600181811c908216806109bb57607f821691505b6020821081036109db57634e487b7160e01b600052602260045260246000fd5b50919050565b808201808211156102d157634e487b7160e01b600052601160045260246000fdfea264697066735822122054ef43f2db413b63b914c68bdc098480eb8352bf7ed260c3efa2422f253241af64736f6c63430008100033";

type MockErc20ConstructorParams =
  | [signer?: Signer]
  | ConstructorParameters<typeof ContractFactory>;

const isSuperArgs = (
  xs: MockErc20ConstructorParams
): xs is ConstructorParameters<typeof ContractFactory> => xs.length > 1;

export class MockErc20__factory extends ContractFactory {
  constructor(...args: MockErc20ConstructorParams) {
    if (isSuperArgs(args)) {
      super(...args);
    } else {
      super(_abi, _bytecode, args[0]);
    }
    this.contractName = "MockErc20";
  }

  deploy(
    overrides?: Overrides & { from?: string | Promise<string> }
  ): Promise<MockErc20> {
    return super.deploy(overrides || {}) as Promise<MockErc20>;
  }
  getDeployTransaction(
    overrides?: Overrides & { from?: string | Promise<string> }
  ): TransactionRequest {
    return super.getDeployTransaction(overrides || {});
  }
  attach(address: string): MockErc20 {
    return super.attach(address) as MockErc20;
  }
  connect(signer: Signer): MockErc20__factory {
    return super.connect(signer) as MockErc20__factory;
  }
  static readonly contractName: "MockErc20";
  public readonly contractName: "MockErc20";
  static readonly bytecode = _bytecode;
  static readonly abi = _abi;
  static createInterface(): MockErc20Interface {
    return new utils.Interface(_abi) as MockErc20Interface;
  }
  static connect(
    address: string,
    signerOrProvider: Signer | Provider
  ): MockErc20 {
    return new Contract(address, _abi, signerOrProvider) as MockErc20;
  }
}
