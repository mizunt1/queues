import scala.collection.immutable.Queue
import java.util.concurrent.locks.{ReentrantLock, Lock}
import java.util.concurrent.atomic.AtomicInteger
import ox.cads.util.Profiler
import ox.cads.testing._
import ox.cads.atomic.AtomicPair
import scala.reflect.ClassTag

class NoLockQueue[T:ClassTag] extends ox.cads.collection.Queue[T]{
  val capacity = 10
  // max number of nodes
  val firstNode = new Node(null.asInstanceOf[T])
  val head = new AtomicReference(firstNode)
  val tail = new AtomicReference(firstNode)
//  var head = new Node()
  tail = head

  var local_head = new AtomicInteger(0)
  var local_tail = new AtomicInteger(0)
  var head_pair = new AtomicPair[Node, AtomicInteger](head, local_head)
  var tail_pair = new AtomicPair[Node, AtomicInteger](tail, local_tail)
  // need to make a pair with data and local_tail?
 
 class Node(){
    val array_size = 10
    val data = new java.util.concurrent.atomic.AtomicReferenceArray[T](array_size)
    @volatile var next : Node = null
    // doesnt have to be atomic as local_tail only handled by enqueue and only 
    //one enqueue can run at one time
  }
  def enqueue(x:T){
    while (true){
      var local_tail_is = local_tail.get()
      var last = tail.get()
      var next = tail.next.get()
      if(local_tail_is == capacity){
        // when we need to create a new node
        var new_node = new Node()
        new_node.data.set(0, x)
        if(next == null){
          if(last.next.compareAndSet(next, new_node)){
            // step 1:now tail.next is new node? from above line?
            tail_pair.compareAndSet((last, capacity),(new_node,1));
            // step 2: now tail is on the new_node and local_tail is 1
            // lin point
            return
          }
        }
        else{
          // some other enqueue has changed tail.next already, next is not null so just set tail to next.
          tail_pair.compareAndSet((last, capacity), (next, 1))
          // x is not enqueued
        }
      }
      else{
      // local head is not at capacity
      // not sure how to set data in array while local_tail is constant
        tail.data.set(local_tail.getAndIncrement(), x);
        // things can go wrong
        return
      }

    }
  }


  def dequeue() : Option[T] = {
    // do i need to make current data array global??
    // not sure how to set and get data with unchanging local_tail and 
    // local_head
    var result:Option[T] = None
    while (true){
      var local_head_is = local_head.get()
      var local_tail_is = local_tail.get()      
      var first = head.get()
      var next  = first.next.get()
      var last = tail.get()
      if(first == head.get()){
        // the case where head is caught up with tail. 
        if(first == last){
          if(local_head_is == local_tail_is){
            if(next == null){
              result = None
            }
            tail.compareAndSet(last, next)}
          // next is not null but first == last, so there must be tail which has not been updated
          }
      }

        // if local head has caught up with local tail, return none
        // and keep local_head there
        // Things that could go wrong
        // these things change above

      else if(local_head_is == capacity){
        // remove node
        // head.next definitely exists as otherwise, head == tail
        if(head.next != null){
          // some other dequeue hasnt removed the next node
          // or there is a next node to begin with
          result = Some(head.data.get(capacity))
          // get the data
          // assuming local_head_is Still capacity

          if(head_pair.compareAndSet((first, capacity),(head.next,0))){
            // we have managed to comfirm that head is still first, and change it to head.next
            // and also confirm that local_tail is capacity and change it to zero
            return result
          }
        }
        else{
          // head.next is null as some dequeuer has already removed the node and we are in head=tail territory
          // we are also in local_head_is = capacity territory so do nothing i
          result = None
        }
      }
      else{
      // local_head is not at capacity
      // head has not changed since start of dequeue
        result = Some(head.data.get(local_head))
        if(head_pair.compareAndSet((first, local_head), (first, local_head.get() + 1)))
          return result

      }
    }
    return result
  }

}


/** Object to perform linearizability testing on a queue. */
object SimpleQueueTest{
  var iters = 200  // Number of iterations by each worker
  val MaxVal = 20 // Maximum value placed in the queue
  var enqueueProb = 0.3 // probability of doing an enqueue
  var queueType = "unbounded" // which queue type are we using?

  // Types of immutable sequential specification queue, undoable sequential
  // specification queue, and concurrent queue.
  type SeqQueue = scala.collection.immutable.Queue[Int]
  type ConcQueue = ox.cads.collection.Queue[Int]
  def seqEnqueue(x: Int)(q: Queue[Int]) : (Unit, Queue[Int]) = 
    ((), q.enqueue(x))
  def seqDequeue(q: Queue[Int]) : (Option[Int], Queue[Int]) =   
    if(q.isEmpty) (None,q) 
    else{ val (r,q1) = q.dequeue; (Some(r), q1) }

  /** A worker for testers based on an immutable sequential datatype. */
  def worker(me: Int, log: GenericThreadLog[SeqQueue, ConcQueue]) = {
    val random = new scala.util.Random(scala.util.Random.nextInt+me*45207)
    for(i <- 0 until iters)
      if(random.nextFloat <= enqueueProb){
	val x = random.nextInt(MaxVal)
	log.log(_.enqueue(x), "enqueue("+x+")", seqEnqueue(x))
      }
      else log.log(_.dequeue, "dequeue", seqDequeue)
  }

  // List of queues
  val queues0 = List("lockFree", "recycle", "unbounded", "mine") 
  val queues = queues0.map("--" + _)

  val usage = 
    """|scala -J-Xmx10g QueueTest
       | [""" + queues.mkString(" | ") + "]\n" +
    """| [--iters n] [--reps n] [--enqueueProb p] [-p n]"""

  def main(args: Array[String]) = {
    // parse arguments
    var verbose = false; var i = 0
    var reps = 1250  // Number of repetitions
    var p = 4      // Number of workers 
    while(i < args.length){
      if(queues.contains(args(i))){ queueType = args(i).drop(2); i += 1 }
      else if(args(i) == "-p"){ p = args(i+1).toInt; i += 2 }
      else if(args(i) == "--iters"){ iters = args(i+1).toInt; i += 2 }
      else if(args(i) == "--reps"){ reps = args(i+1).toInt; i += 2 }
      else if(args(i) == "--enqueueProb"){ 
	enqueueProb = args(i+1).toDouble; i += 2 
      }
      else sys.error("Usage:\n"+usage.stripMargin)
    }

    // Now run the tests
    val t0 = java.lang.System.nanoTime
    var r = 0
    var result = 1
    while(r < reps && result > 0){
      // The sequential and concurrent queue
      val seqQueue = Queue[Int]()
      val concQueue : ConcQueue = queueType match{
        case "lockFree" =>  new ox.cads.collection.LockFreeQueue[Int]
        case "recycle" => new ox.cads.collection.LockFreeQueueRecycle[Int](p)
        case "unbounded" => new ox.cads.collection.UnboundedQueue[Int]
        case "mine" => new NoLockQueue[Int]
      }

      // Create and run the tester object
      val tester = LinearizabilityTester.JITGraph[SeqQueue, ConcQueue](
        seqQueue, concQueue, p, worker, iters)
      result = tester()
      r += 1
      if(r%100 == 0) print(".")
    } // end of for loop
    val t1 = java.lang.System.nanoTime
    println("\nTime taken: "+(t1-t0)/1000000+"ms")
  }
  
}
