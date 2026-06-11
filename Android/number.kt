private fun isContinuousDigits(pin: String): Boolean {
    if (pin.length != 4) return false
    if (!pin.all { it.isDigit() }) return false

    val digits = pin.map { it - '0' }
    
    val increasing = digits.zipWithNext().all { (a, b) ->
        b == (a + 1) % 10
    }

    val decreasing = digits.zipWithNext().all { (a, b) ->
        b == (a + 9) % 10
    }

    return increasing || decreasing
}
