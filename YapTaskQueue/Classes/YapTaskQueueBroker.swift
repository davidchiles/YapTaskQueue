//
//  YapTaskQueueExtension.swift
//  YapTaskQueue
//
//  Created by David Chiles on 3/31/16.
//  Copyright © 2016 David Chiles. All rights reserved.
//

import Foundation
import YapDatabase.YapDatabaseFilteredView
import YapDatabase

internal enum QueueState {
    case Processing(YapTaskQueueAction)
    case Paused(YapTaskQueueAction,NSDate)
}

public enum DatabaseStrings:String {
    case YapTasQueueMasterBrokerExtensionName = "YapTasQueueMasterBrokerExtensionName"
}

@objc public protocol YapTaskQueueHandler {
    
    /** This method is called when an item is available to be exectued. Call completion once finished with the action item.
     
     */
    func handleNextItem(action:YapTaskQueueAction, completion:(success:Bool, retryTimeout:NSTimeInterval)->Void)
}

/// YapTaskQueueBroker is a subclass of YapDatabaseFilteredView. It listens for changes and manages the timing of running an action.
public class YapTaskQueueBroker: YapDatabaseFilteredView {
    
    /// YapDatabaseConnection for listening to and getting database objects and changes.
    private var databaseConnection:YapDatabaseConnection? = nil
    
    /// This dictionary tracks what is currently going on so that only one item at a time is being executed.
    private var currentState = [String:QueueState]()
    
    /// This queue is used to guarentee that changes to the current work dictionary don't have any queue issues.
    private let isolationQueue = dispatch_queue_create("YapTaskQueueExtension", DISPATCH_QUEUE_SERIAL)
    
    /// This queue is just used for receiving in the NSNotifications
    private let notificationQueue = NSOperationQueue()
    
    /// This is where most operations in this class start on.
    private let workQueue = dispatch_queue_create("YapTaskQueueBroker-GCDQUEUE", DISPATCH_QUEUE_SERIAL)
    
    /// This is the Object that completes the operation and is handed a task to complete.
    public var handler:YapTaskQueueHandler
    
    /**
     Create a new YapTaskQueueBroker. After created the YapTaskQueueBroker needs to be registered with a database in order to get changes and objects.
     
     - parameters:
     - parentViewName: Should be the view name of the YapTaskQueueMasterBroker
     - handler: The handler that will be passed the items
     - filtering: This block should return true if this broker should handle a particular queue. Each queue is executed synchronously
     
     */
    internal init(parentViewName viewName: String, handler:YapTaskQueueHandler, filtering: (queueName:String) -> Bool) {
        self.notificationQueue.maxConcurrentOperationCount = 1
        self.handler = handler
        let filter = YapDatabaseViewFiltering.withKeyBlock { (transaction, group, collection, key) -> Bool in
            //Check if this group has already been 'claimed' by a queue broker
            return filtering(queueName: group)
        }
        super.init(parentViewName: viewName, filtering: filter, versionTag: nil, options:nil)
        
        // Best way for now to find when we are registred with a database
        self.addObserver(self, forKeyPath: "registeredDatabase", options: .New, context: nil)
    }
    
    /**
     Create a new YapTaskQueueBroker. This uses the view name as  prefix for all queues it handles.
     For example if the viewName is "MessageQueue" this will hanldle all queues that begin with "MessageQueue" like "MessageQueue-buddy1"
     
     - parameters:
     - parentViewName: Should be the view name of the YapTaskQueueMasterBroker
     - handler: The handler that will be passed the items
     */
    public convenience init(parentViewName viewName: String, name:String, handler:YapTaskQueueHandler) {
        
        self.init(parentViewName:viewName,handler:handler,filtering: { quename in
            return quename.hasPrefix(name)
        })
    }
    
    /// For now this seems to be the best way to find out with we are registred with the database
    override public func observeValueForKeyPath(keyPath: String?, ofObject object: AnyObject?, change: [String : AnyObject]?, context: UnsafeMutablePointer<Void>) {
        self.didRegisterExtension()
    }
    
    /// Only way to get the current action for a given queue
    internal func actionForQueue(queue:String) -> YapTaskQueueAction? {
        
        
        //Ensure that some value was stored
        guard let currentState = self.stateForQueue(queue) else {
            return nil
        }
        
        // Only get action if the state was .Processing
        switch currentState {
        case .Processing(let action):
            return action
        default:
            return nil
        }
    }
    
    /// Only way to get the current state for a given queue
    internal func stateForQueue(queue:String) -> QueueState? {
        var state:QueueState? = nil
        
        // Use isolation queue to access self.currentState
        dispatch_sync(self.isolationQueue) {
            state = self.currentState[queue]
        }
        
        return state
    }
    
    /// Only way to set or remove an action from the active action dictionary
    internal func setQueueState(state:QueueState?, queueName:String) {
        dispatch_async(self.isolationQueue) {
            if let act = state {
                self.currentState.updateValue(act, forKey: queueName)
            } else {
                self.currentState.removeValueForKey(queueName)
            }
        }
    }
    
    /// Execute on registration with a database
    func didRegisterExtension() {
        
        NSNotificationCenter.defaultCenter().addObserverForName(YapDatabaseModifiedNotification, object: self.registeredDatabase, queue: self.notificationQueue) {[weak self] (notification) in
            if let strongSelf = self {
                strongSelf.processDatabaseNotification(notification)
            }
        }
        
        self.databaseConnection = self.registeredDatabase?.newConnection()
    }
    
    /// Takes in a datbase notification and checks if there are any updates for this view
    func processDatabaseNotification(notification:NSNotification) {
        
        guard let viewName = self.registeredName else {
            return
        }
        
        guard let connection = self.databaseConnection?.ext(viewName) as? YapDatabaseViewConnection else {
            return
        }
        
        if connection.hasChangesForNotifications([notification]) {
            
            
            
            self.checkForActions()
        }
    }
    
    /** Goes through all the queues and checks if there are more actions to execute */
    func checkForActions() {
        var groups:[String]? = nil
        self.databaseConnection?.asyncReadWriteWithBlock({ (transaction) in
            guard let name = self.registeredName else {
                return
            }
            
            guard let viewTransaction = transaction.ext(name) as? YapDatabaseViewTransaction else {
                return
            }
            groups = viewTransaction.allGroups()
            
            }, completionQueue: self.workQueue, completionBlock: {
                if let groupsArray = groups {
                    
                    // We need to check all our processing and paused actions to see if their queues are still there.
                    // If the group is gone then we need to reset the state on that group
                    let keys = self.currentState.keys
                    for group in keys where !groupsArray.contains(group) {
                        self.setQueueState(nil, queueName: group)
                    }
                    
                    
                    for groupName in groupsArray {
                        self.checkQueue(groupName)
                    }
                }
                
        })
    }
    
    /** Go through a queue and check if there is an item to execute and no other action form that queue being executed. */
    func checkQueue(queueName:String) {
        var newAction:YapTaskQueueAction? = nil
        self.databaseConnection?.readWithBlock({ (transaction) in
            guard let name = self.registeredName else {
                return
            }
            
            guard let viewTransaction = transaction.ext(name) as? YapDatabaseViewTransaction else {
                return
            }
            
            // Check if we have an object in the queue.
            // If we have no object then clear out the state
            // If we do have an object it needs to match the item being processed or paused. There can be a mismatch if the item it deleted from the database is deleted.
            // If it's deleted then we should force ourselves to move to the next item.
            if let action = viewTransaction.firstObjectInGroup(queueName) as? YapTaskQueueAction {
                switch self.stateForQueue(queueName) {
                case let .Some(.Paused(item,_)) where item.yapKey() != action.yapKey() || item.yapCollection() != action.yapCollection():
                    newAction = action
                case let .Some(.Processing(item)) where item.yapKey() != action.yapKey() || item.yapCollection() != action.yapCollection():
                    newAction = action
                case .None:
                    newAction = action
                default:
                    break
                }
            } else {
                self.setQueueState(nil, queueName: queueName)
            }
        })
        
        // If there is a new action then we send it to be handled and update the queue state otherwise we can stop
        // because the queue is doing something or paused
        guard let action = newAction else {
            return
        }
        
        //Set the state correctly as we're about to start processing a new action
        self.setQueueState(.Processing(action), queueName: queueName)
        
        dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), {
            self.handler.handleNextItem(action, completion: { [weak self] (result, retryTimeout) in
                if let strongSelf = self {
                    if result {
                        strongSelf.databaseConnection?.readWriteWithBlock({ (t) in
                            t.removeObjectForKey(action.yapKey(), inCollection: action.yapCollection())
                            strongSelf.setQueueState(nil, queueName: queueName)
                        })
                        
                        dispatch_async(strongSelf.workQueue, { [weak strongSelf] in
                            strongSelf?.checkQueue(queueName)
                            })
                        
                        
                    } else if retryTimeout == 0.0 {
                        strongSelf.checkQueue(queueName)
                    } else {
                        let date = NSDate(timeIntervalSinceNow: retryTimeout)
                        strongSelf.setQueueState(.Paused(action,date), queueName: queueName)
                        if retryTimeout > 0 && retryTimeout < (Double(INT64_MAX) / Double(NSEC_PER_SEC)) {
                            let time = dispatch_time(DISPATCH_TIME_NOW, Int64(retryTimeout * Double(NSEC_PER_SEC)))
                            dispatch_after(time,strongSelf.workQueue,{ [weak strongSelf] in
                                strongSelf?.restartQueueIfPaused(queueName)
                                })
                        }
                        
                    }
                    
                }
                })
            
        })
    }
    
    func restartQueueIfPaused(queueName:String) {
        //Remove paused state
        if let state = self.stateForQueue(queueName) {
            switch state {
            case .Paused:
                self.setQueueState(nil, queueName: queueName)
            default:
                break
            }
        }
        self.checkQueue(queueName)
    }
    
    public func restartQueues(queueNames:[String]) {
        for name in queueNames {
            self.restartQueueIfPaused(name)
        }
    }
    
    deinit {
        self.removeObserver(self, forKeyPath: "registeredDatabase")
    }
    
}
extension YapTaskQueueBroker {
    /**
     Creates a string that is a queue name that is handled by this queue broker. If this queue broker name is "MessageQueue" then passing "buddy1" as the suffix
     yields "MessageQueue-buddy1"
     
     -parameter suffix: The suffix of the queue name
     -returns: A queue name that is unique to that suffix and handled by this queue broker
     */
    public func queueNameWithSuffix(suffix:String) throws ->  String {
        guard let name = self.registeredName else {
            throw YapTaskQueueError.NoRegisteredViewName
        }
        return "\(name)-\(suffix)"
    }
}

//MARK: Class Methods
extension YapTaskQueueBroker {
    ///Use this method for convience. It automatically ensures a Master Broker is setup and registers the needed database views
    public class func setupWithDatabase(database:YapDatabase, name:String, handler:YapTaskQueueHandler) throws -> Self
    {
        return try setupWithDatabaseHelper(database, name: name, handler: handler)
    }
    
    public class func setupWithDatabaseHelper<T>(database:YapDatabase, name:String, handler:YapTaskQueueHandler) throws -> T
    {
        if database.registeredExtension(DatabaseStrings.YapTasQueueMasterBrokerExtensionName.rawValue) == nil {
            let masterBroker = YapTaskQueueMasterBroker(options: nil)
            let result = database.registerExtension(masterBroker, withName: DatabaseStrings.YapTasQueueMasterBrokerExtensionName.rawValue)
            if !result {
                throw YapTaskQueueError.CannotRegisterMasterView
            }
        }
        
        let queue = YapTaskQueueBroker(parentViewName: DatabaseStrings.YapTasQueueMasterBrokerExtensionName.rawValue, name:name, handler: handler)
        if !database.registerExtension(queue, withName: name) {
            throw YapTaskQueueError.CannotRegisterBrokerView
        }
        return queue as! T
    }
}
