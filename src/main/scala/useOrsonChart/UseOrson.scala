package useOrsonChart

import java.awt.BorderLayout
import javax.swing.{JPanel, JFrame}

import com.orsoncharts.graphics3d.swing.DisplayPanel3D
import com.orsoncharts.plot.CategoryPlot3D
import com.orsoncharts.renderer.category.StackedBarRenderer3D
import com.orsoncharts.{Chart3DPanel, Chart3DFactory, Chart3D}
import com.orsoncharts.data.category.{StandardCategoryDataset3D, CategoryDataset3D}

/**
  * Created by kaiyin on 2/5/16.
  */
object UseOrson extends JFrame {
  def createDataset(): CategoryDataset3D = {
    val dataset: StandardCategoryDataset3D = new StandardCategoryDataset3D();
    dataset.addValue(146, "Survivors", "Women/Children", "1st");
    dataset.addValue(104, "Survivors", "Women/Children", "2nd");
    dataset.addValue(103, "Survivors", "Women/Children", "3rd");
    dataset.addValue(20, "Survivors", "Women/Children", "Crew");
    dataset.addValue(57, "Survivors", "Men", "1st");
    dataset.addValue(14, "Survivors", "Men", "2nd");
    dataset.addValue(75, "Survivors", "Men", "3rd");
    dataset.addValue(192, "Survivors", "Men", "Crew");
    dataset.addValue(4, "Victims", "Women/Children", "1st");
    dataset.addValue(13, "Victims", "Women/Children", "2nd");
    dataset.addValue(141, "Victims", "Women/Children", "3rd");
    dataset.addValue(3, "Victims", "Women/Children", "Crew");
    dataset.addValue(118, "Victims", "Men", "1st");
    dataset.addValue(154, "Victims", "Men", "2nd");
    dataset.addValue(387, "Victims", "Men", "3rd");
    dataset.addValue(670, "Victims", "Men", "Crew");
    return dataset;
  }

  def createDemoPanel(): JPanel = {
    val content: JPanel = new JPanel(new BorderLayout())
    val dataset3D = createDataset()
    val chart: Chart3D = Chart3DFactory.createStackedBarChart(
      "Titanic", "Survival data for 2,202 passengers",
      //      dataset3D, null, "Class", "Passengers"
      dataset3D, null, null, null
    )
    val plot: CategoryPlot3D = chart.getPlot.asInstanceOf[CategoryPlot3D]
    val renderer = plot.getRenderer.asInstanceOf[StackedBarRenderer3D]
    val chartPanel: Chart3DPanel = new Chart3DPanel(chart)
    content.add(new DisplayPanel3D(chartPanel))
    content
  }

  this.getContentPane.add(createDemoPanel())

  def main(args: Array[String]): Unit = {
    this.pack()
    this.setVisible(true)
  }

}
