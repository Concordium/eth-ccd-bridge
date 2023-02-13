import axios from "axios";

const usePrice = () => {
  const getPrice = async (token: "ETH" | "CCD") => {
    if (token === "ETH") {
      const ethResponse = await axios.get(
        "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd"
      );
      return ethResponse.data.ethereum.usd;
    } else {
      const ethResponse = await axios.get(
        "https://api.coingecko.com/api/v3/simple/price?ids=concordium&vs_currencies=usd"
      );
      return ethResponse.data.concordium.usd;
    }
  };

  return getPrice;
};

export default usePrice;
