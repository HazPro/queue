import * as q from 'amqplib'
import * as _ from 'lodash'

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
    const taskId = task.id || 'random generation id'
    task.id = taskId
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
            this.channel.cancel(consumerTag)
          return resolve(msg)
        } else {
          this.channel.nack(msg, false, true)
        }
      })
    })
  }
  async waitTaskType(type: string): Promise<q.ConsumeMessage> {
    return new Promise(async (resolve, reject) => {
      await this.channel.consume(type, (msg: q.ConsumeMessage) => {
        // console.log(consume)
        const consumerTag: any = _.get(msg.fields, 'consumerTag')
        if (!consumerTag)
          this.channel.cancel(consumerTag)
        return resolve(msg)
      })
    })
  }
  async ack(msg: q.ConsumeMessage) {
    this.channel.ack(msg)
  }
}