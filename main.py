import asyncio
from aiodecorators import Semaphore
from web3 import AsyncWeb3, AsyncHTTPProvider
from ens import AsyncENS
import pandas as pd


w3 = AsyncWeb3(AsyncHTTPProvider("https://mainnet.infura.io/v3/#"))
ns = AsyncENS.from_web3(w3)
df = pd.read_csv("download-query_run_results.csv")
df = df.sort_values(by="BLOCK_NUMBER").reset_index(drop=True)
res = []


@Semaphore(50)
async def run(i):
    data = df.iloc[i].copy()
    addr = data["ETH_FROM_ADDRESS"]
    try:
        name = await ns.name(addr)
        if name != None:
            print(f"{i} {addr} found name: {name}")
            data["ENS"] = name
            res.append(data)
    except Exception as e:
        print("AsyncENS error: ", e)

    await asyncio.sleep(0.1)


async def main():
    await asyncio.gather(*[run(i) for i in range(df.shape[0])])
    df_new = pd.DataFrame(res)
    df_new.to_csv("pauly0x_ens.csv")


if __name__ == "__main__":
    asyncio.run(main())