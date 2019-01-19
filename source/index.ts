import * as q from 'amqplib'
import * as _ from 'lodash'
import { v4 } from 'uuid'

export interface IQueueTask {
  id?: string,
  queueId: string,
  type: string,
  body: any
}
export interface IQueueConfig {
  url: string,
  exchangeName: string,
  resultQueue: string
}
export interface IMessage {
  body: IQueueTask,
  ack: Function,
  nack: Function
}
export class Queue {

  private queue: q.Connection
  private channel: q.Channel
  private config: IQueueConfig

  constructor(config: IQueueConfig) {
    this.config = config
  }
  async open() {
    this.queue = await q.connect(this.config.url)
    this.channel = await this.queue.createChannel()
  }
  /**
   * Отправить в очередь на выполенение
   * и дождаться результата
   */
  async pushTaskAndWaitResult(task: IQueueTask): Promise<any> {
    const taskId = this.pushTask(task)
    return await this.waitResultTask(taskId)
  }
  /**
   * Отправить в очередб на выполенеие
   * результат не ожидается
   */
  async pushTask(task: IQueueTask): Promise<any> {
    console.log(task)
    const taskId = task.id || v4()
    task.id = taskId
    console.log(task)
    await this.channel.assertExchange(this.config.exchangeName, 'direct', { durable: true })
    await this.channel.assertQueue(task.type)
    await this.channel.bindQueue(task.type, this.config.exchangeName, task.type)
    await this.channel.publish(
      this.config.exchangeName,
      task.type,
      Buffer.from(JSON.stringify(task))
    )
    return taskId
  }
  /**
   * Ожидаем задачу с типом
   */
  async waitResultTask(taskId: any): Promise<q.ConsumeMessage> {
    return new Promise(async (resolve, reject) => {
      await this.channel.consume(this.config.resultQueue, (msg: q.ConsumeMessage) => {
        const body = JSON.parse(msg.content.toString('utf8'))
        if (body.id == taskId) {
          const consumerTag: any = _.get(msg.fields, 'consumerTag')
          if (!consumerTag)
            this.cancelConsume(consumerTag)
          return resolve(msg)
        } else {
          this.channel.nack(msg, false, true)
        }
      })
    })
  }
  async waitTaskType(type: string): Promise<IMessage> {
    return new Promise(async (resolve, reject) => {
      await this.channel.consume(type, async (msg: q.ConsumeMessage) => {
        const consumerTag: any = _.get(msg.fields, 'consumerTag')
        if (!consumerTag)
          await this.cancelConsume(consumerTag)
        const body = JSON.parse(msg.content.toString('utf8'))
        const message: IMessage = {
          body: body as IQueueTask,
          nack: this.channel.nack.bind(this.channel, msg),
          ack: this.channel.ack.bind(this.channel, msg)
        }
        return resolve(message)
      })
    })
  }
  async consumeQueue(type: string, handler: (msg: IMessage) => any): Promise<string> {
    await this.channel.assertExchange(this.config.exchangeName, 'direct', { durable: true })
    await this.channel.assertQueue(type)
    await this.channel.bindQueue(type, this.config.exchangeName, type)
    const consume = await this.channel.consume(type, msg => {
      const body = JSON.parse(msg.content.toString('utf8'))
      const message: IMessage = {
        body: body as IQueueTask,
        nack: this.channel.nack.bind(this.channel, msg),
        ack: this.channel.ack.bind(this.channel, msg)
      }
      handler(message)
    })
    return consume.consumerTag
  }
  async ack(msg: q.ConsumeMessage) {
    this.channel.ack(msg)
  }

  async cancelConsume(tag: string) {
    await this.channel.cancel(tag)
  }

}