import asyncio
import aiohttp


#Realiza una llamada asincrona y obtiene la respuesta como json
async def realizar_solicitud(sesion, url, headers):
    async with sesion.get(url['url'], headers=headers) as response:
        if response.status == 200:
            data = await response.json()

            valores = data["included"][0]["attributes"]["values"]

            resultado = {"valores": valores}

            resultado['cod_comunidad'] = url['cod_comunidad']
            resultado['nombre_comunidad'] = url['nombre_comunidad']
            resultado['anio'] = url['anio']

            return resultado
        
        else:
            print(f"Error en {url}: {response.status}")


#Realiza una lista de llamadas y las ejecuta asincronamente
async def realizar_solicitudes(sesion, urls, headers):
    async with aiohttp.ClientSession() as sesion:
        llamadas = [realizar_solicitud(sesion, url, headers) for url in urls]

        resultado = await asyncio.gather(*llamadas)
        
        return resultado