/*
 * SanKir Technologies
 * (c) Copyright 2020.  All rights reserved.
 * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
 *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
 *  without the prior written permission from Sankir Technologies.
 *
 * The course contents can be accessed by subscribing to pro-Spark course.
 *
 * Please visit www.sankir.com for details.
 *
 */

package com.sankir.smp.vo

case class BigTableErrorRows(errorReplay: String = "",
                             timestamp: String,
                             errorType: String,
                             payload: String,
                             jobName: String,
                             errorMessage: String,
                             stackTrace: String)
