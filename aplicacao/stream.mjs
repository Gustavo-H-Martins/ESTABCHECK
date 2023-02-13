import { createServer, request } from 'http'
import { createReadStream } from 'fs'
import { setTimeout } from 'timers/promises'
import { Transform, Readable } from 'stream'
import { TransformStream, WritableStream } from 'stream/web'
import express, { response } from 'express'
import csvtojson from 'csvtojson'
import fs from 'node:fs'
import path from 'path'

const app = express()
const PORT = 3000
const arquivo_diretorio = '../br_base/Bares e outros estabelecimentos especializados em servir bebidas com entretenimento.csv'

app.get('/estabelecimentos/estado=:estado', async (request, response) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': '*',
  }
  if (request.method === 'OPTIONS') {
    response.writeHead(204, headers)
    response.end()
    return
  }

  let items = 0
  request.once('close', () => console.log('connection was closed!', items))

  const estado = request.params.estado.toUpperCase();
  
  Readable.toWeb(createReadStream(arquivo_diretorio))
    .pipeThrough(Transform.toWeb(csvtojson({ delimiter: ';' })))
    .pipeThrough(
      new TransformStream({
        transform(chunk, controller) {
          const dados = JSON.parse(Buffer.from(chunk))
          if (dados.ESTADO === estado) {
            controller.enqueue(
              JSON.stringify(dados).concat('\n')
            )
          }
        },
      })
    )
    .pipeTo(
      new WritableStream({
        async write(chunk) {
          //await setTimeout(200)
          items++
          response.write(chunk)
        },
        close() {
          response.end()
        }
      })
    )

  response.writeHead(200, headers)
})


app.get('/teste', async (request, response) => {
  let items = 0
  request.once('close', () => console.log('connection was closed!', items))
  response.writeHead(200, { 'Content-Type': 'application/json' })

  const folderPath = '../br_base';

  fs.readdir(folderPath, (err, files) => {
    if (err) {
      console.error(err);
      response.end();
      return;
    }

    const csvFiles = files.filter(file => path.extname(file) === '.csv');

    let csvFileStreams = csvFiles.map(file => {
      const filePath = path.join(folderPath, file);
      return fs.createReadStream(filePath);
    });

    Readable.toWeb(Readable.from(csvFileStreams))
    .pipeThrough(Transform.toWeb(csvtojson()))
    .pipeThrough(
      new TransformStream({
        transform(chunk, controller) {
          const data = JSON.parse(Buffer.from(chunk))

          controller.enqueue(JSON.stringify(chunk).concat('\n'))
          items++
        },
      })
    )
    .pipeTo(
      new WritableStream({
        async write(chunk) {
          await setTimeout(200)
          response.write(chunk)
        },
        close() {
          response.end()
        }
      })
    )
  });
});

createServer(app)
  .listen(PORT)
  .on('listening', _ => console.log('server running at ', PORT))
