// version corrected by hanno
import scala.collection.immutable.Queue
import java.util.concurrent.locks.{ReentrantLock, Lock}
import java.util.concurrent.atomic.{AtomicInteger,AtomicReference}
import ox.cads.util.Profiler
import ox.cads.testing._
import ox.cads.atomic.AtomicPair
import scala.reflect.ClassTag

class NoLockQueue[T:ClassTag] extends ox.cads.collection.Queue[T]{
  val capacity = 10 // max number of elements in array
  val firstNode = new Node()

  val head_pair = new AtomicPair[Node, Int](firstNode, 0)
  val tail_pair = new AtomicPair[Node, Int](firstNode, 0)
 
 class Node(){
    val data = new java.util.concurrent.atomic.AtomicReferenceArray[T](capacity)
    val next = new AtomicReference(null.asInstanceOf[Node])
  }

  def enqueue(x:T){
    while (true){
      val (tail,local_tail) = tail_pair.get
      if(local_tail == capacity){
        // when we need to create a new node
        val new_node = new Node()
        new_node.data.set(0, x)
        if(tail.next.compareAndSet(null, new_node)){
          // step 1:now tail.next is new node? from above line?
          tail_pair.compareAndSet((tail, capacity),(new_node,1));
          // step 2: now tail is on the new_node and local_tail is 1
          // linearization point
          return
        } else {
          // some other enqueue has changed tail.next already, next is not null so just set tail to next.
          tail_pair.compareAndSet((tail, capacity), (tail.next.get, 1))
          //linearization point
          // x is not enqueued
        }
      }
      else{
      // local head is not at capacity
        if(tail.data.compareAndSet(local_tail, null.asInstanceOf[T], x)) {
          // advance tail_pair
          tail_pair.compareAndSet((tail,local_tail),(tail,local_tail+1))
          // linearization point
          return
        } else { // someone else used this slot, advanve tail_pair and try again
          tail_pair.compareAndSet((tail,local_tail),(tail,local_tail+1))        
          // linearization point
        }
      }
    }
  }


  def dequeue() : Option[T] = {
    var result:Option[T] = None
    while (true){
      val (head, local_head) = head_pair.get
      var (tail, local_tail) = tail_pair.get      
      // the case where head_pair is caught up with tail_pair. 
      if(head == tail && local_head == local_tail){
        return None
      }
      else if(local_head == capacity){
      // try and advance head and retry
        head_pair.compareAndSet((head, capacity),(head.next.get,0))
        // linearization point
      }
      else{
      // local_head is not at capacity
        result = Some(head.data.get(local_head))
        if(head_pair.compareAndSet((head, local_head), (head, local_head + 1)))
          // linearization point
          return result
        // else someone else got this datum, retry
      }
    }
    assert(false) // this point is never reached!
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
