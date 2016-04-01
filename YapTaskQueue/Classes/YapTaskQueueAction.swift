//
//  YapTaskQueueAction.swift
//  YapTaskQueue
//
//  Created by David Chiles on 3/31/16.
//  Copyright © 2016 David Chiles. All rights reserved.
//

import Foundation

public typealias YapAction = () throws -> Bool

public protocol YapTaskQueueAction {
    func action() -> YapAction?
    func queueName() -> String?
    func sort(otherObject:YapTaskQueueAction) -> NSComparisonResult
}