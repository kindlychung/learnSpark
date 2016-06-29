package usexchart

import org.knowm.xchart.{SwingWrapper, QuickChart}

/**
  * Created by kaiyin on 2/5/16.
  */
object UseXchart {
  def main(args: Array[String]) {
    val xData = Array(0.0, 1, 2)
    val yData = Array(2.0, 1, 0)
    val chart = QuickChart.getChart("title", "x", "y", "function", xData, yData)
    var swingChart: SwingWrapper = new SwingWrapper(chart)
    swingChart.displayChart()
  }
}
