import OpenAPIClientAxios from "openapi-client-axios";
import { GetState, SetState } from "zustand";
import { Client } from "../../api-query/__generated__/AxiosClient";
import {
  AxiosClientActions,
  AxiosClientStore,
} from "../../types/store/axios-client";

const axiosClientActions = (
  set: SetState<AxiosClientStore>,
  get: GetState<AxiosClientStore>
): AxiosClientActions => ({
  getClient: async () => {
    let client = get().client;
    if (!client) {
      const api = new OpenAPIClientAxios({
        definition: `${process.env.NEXT_PUBLIC_API_URL}/openapi.json`,
        axiosConfigDefaults: {
          baseURL: process.env.NEXT_PUBLIC_API_URL,
        },
      });
      client = await api.init<Client>();
      client?.interceptors.request.use((request) => {
        const token =
          localStorage["profile"] && JSON.parse(localStorage["profile"])?.token;
        if (token && request?.headers) {
          request.headers["Authorization"] = `Bearer ${token}`;
        }
        return request;
      });
      set((state) => ({ ...state, client }));
    }
    return client as Client;
  },
});

export default axiosClientActions;