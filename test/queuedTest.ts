import { Queue, IQueueConfig, IQueueTask } from '../source'

describe('Queued test', () => {
  let queue: Queue = null
  let config: IQueueConfig
  before(async () => {
    config = {
      url: "amqp://gqvnsede:PXNexiA9coQRT8wNa-K1NzHsQ0cNrLbP@baboon.rmq.cloudamqp.com/gqvnsede",
      exchangeName: 'queueExc',
      resultQueue: 'result'
    }
    queue = new Queue(config)
    await queue.open()
  })
  it('push task', async () => {
    const task: IQueueTask = {
      queueId: 'a nshl askld asdlk',
      body: { a: 1 },
      type: 'report'
    }
    const t = await queue.pushTask(task)
    const s = await queue.waitTaskType('report')
    console.log(s.body)
    s.ack()
  })
})