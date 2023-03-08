const addresses = {
    /**
     * ETH Address
     */
    eth: process.env.NEXT_PUBLIC_ETH_TOKEN_ADDRESS || "",

    /**
     * Root Manager Address
     */
    root: process.env.NEXT_PUBLIC_ROOT_MANAGER_ADDRESS || "",

    /**
     * Bridge Manager Address
     */
    bridgeManager: {
        index: process.env.NEXT_PUBLIC_BRIDGE_MANAGER_INDEX,
        subindex: process.env.NEXT_PUBLIC_BRIDGE_MANAGER_SUBINDEX ?? "0",
    },
};

export default addresses;
