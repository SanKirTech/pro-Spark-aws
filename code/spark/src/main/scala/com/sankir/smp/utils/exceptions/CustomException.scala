/*
 *
 *  * SanKir Technologies
 *  * (c) Copyright 2021.  All rights reserved.
 *  * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
 *  *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
 *  *  without the prior written permission from Sankir Technologies.
 *  *
 *  * The course contents can be accessed by subscribing to pro-Spark course.
 *  *
 *  * Please visit www.sankir.com for details.
 *  *
 *
 */

package com.sankir.smp.utils.exceptions

/***
  *
  * @param message takes in a error string and prints exception
  */
case class SchemaValidationFailedException(message: String)
    extends Exception(message)

/***
  *
  * @param message takes in a error string and prints exception
  */
case class BusinessValidationFailedException(message: String)
    extends Exception(message)
