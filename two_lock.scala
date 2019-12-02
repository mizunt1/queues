import scala.collection.immutable.Queue
import java.util.concurrent.locks.{ReentrantLock, Lock}
import java.util.concurrent.atomic.AtomicInteger
import ox.cads.util.Profiler
import ox.cads.testing._
import scala.reflect.ClassTag


class LinkedArrayList[T:ClassTag] extends ox.cads.collection.Queue[T]{
  // make two Reentrant locks, one for enq, one for deq
  val capacity = 10
  // max number of nodes
  var enqLock = new ReentrantLock()
  var deqLock = new ReentrantLock()
  var notFullCondition = enqLock.newCondition()
  var notEmptyCondition = deqLock.newCondition()
  // create dummy nodes? head and tail
  // not sure why tail = head
  var head = new Node()
  var tail = head
 
 class Node(){
    val array_size = 10
    val data = new java.util.concurrent.atomic.AtomicReferenceArray[T](array_size)
    @volatile var next : Node = null
    var local_tail = 0
    var local_head = 0
    // doesnt have to be atomic as local_tail only handled by enqueue and only 
    //one enqueue can run at one time
  }

  def enqueue(x:T){
    enqLock.lock()
    try {
      if(tail.local_tail == capacity){
        // if index is 10, create new node and insert 
        // if local_tail is full capacity, change to zero and add new node
        var new_node = new Node()
        new_node.data.set(0, x);
        new_node.local_tail = 1;
        tail.next  = new_node;
        tail = new_node;
        //linearisation point
      }
      else{
        // situation where we dont need to create a new node
        tail.data.set(tail.local_tail, x);
          // if the above succeeds, increment local_tail
          // local_tail gives last index not filled
        tail.local_tail +=1;
      }
    }
    finally {
      enqLock.unlock();
    }  
  }

  def dequeue() : Option[T] = {
    var result:Option[T] = None
    deqLock.lock()
    try {
      if(head == tail && head.local_head == tail.local_tail)
        // if local head has caught up with local tail, return none
        // and keep local_head there
      {
        result = None
      }

      else if(head.local_head == capacity-1){
        // if capacity is 4, and local_head is at 3, we must dequeue this item and 
        // remove node
        result = Some(head.data.get(head.local_head));
        // head.next definitely exists as otherwise, head == tail
        head = head.next;
        // must reduce the number of nodes by 1
        head.local_head = 0
        // next item to be dequeued will be at index 
      }
      else{
        result = Some(head.data.get(head.local_head))
        head.local_head +=1
      }
    }
    finally {
      deqLock.unlock();
    }
    return result
  }

}
/* A linearizability tester for total queues (i.e. queues that do not 
 * block, and such that dequeues on the empty queue return None). */



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
        case "mine" => new LinkedArrayList[Int]
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
