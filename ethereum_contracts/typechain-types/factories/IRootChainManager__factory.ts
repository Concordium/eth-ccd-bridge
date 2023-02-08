/* Autogenerated file. Do not edit manually. */
/* tslint:disable */
/* eslint-disable */

import { Contract, Signer, utils } from "ethers";
import { Provider } from "@ethersproject/providers";
import type {
  IRootChainManager,
  IRootChainManagerInterface,
} from "../IRootChainManager";

const _abi = [
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
    ],
    name: "cleanMapToken",
    outputs: [],
    stateMutability: "nonpayable",
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
        name: "ccdAddress",
        type: "bytes32",
      },
    ],
    name: "depositEtherFor",
    outputs: [],
    stateMutability: "payable",
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
        name: "ccdAddress",
        type: "bytes32",
      },
      {
        internalType: "address",
        name: "rootToken",
        type: "address",
      },
      {
        internalType: "bytes",
        name: "depositData",
        type: "bytes",
      },
    ],
    name: "depositFor",
    outputs: [],
    stateMutability: "payable",
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
    name: "mapToken",
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
    name: "registerVault",
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
    name: "remapToken",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function",
  },
  {
    inputs: [
      {
        components: [
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
            internalType: "address payable",
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
        internalType: "struct IRootChainManager.WithdrawParams",
        name: "withdrawParams",
        type: "tuple",
      },
      {
        internalType: "bytes32[]",
        name: "proof",
        type: "bytes32[]",
      },
    ],
    name: "withdraw",
    outputs: [],
    stateMutability: "payable",
    type: "function",
  },
];

export class IRootChainManager__factory {
  static readonly abi = _abi;
  static createInterface(): IRootChainManagerInterface {
    return new utils.Interface(_abi) as IRootChainManagerInterface;
  }
  static connect(
    address: string,
    signerOrProvider: Signer | Provider
  ): IRootChainManager {
    return new Contract(address, _abi, signerOrProvider) as IRootChainManager;
  }
}